package weed_server

import (
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"

	"golang.org/x/net/context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) GetMaster() string {
	return vs.currentMaster
}
func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with seed master nodes: %v", vs.SeedMasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.volume")

	var err error
	var newLeader string
	for {
		for _, master := range vs.SeedMasterNodes {
			if newLeader != "" {
				master = newLeader
			}
			masterGrpcAddress, parseErr := pb.ParseServerToGrpcAddress(master)
			if parseErr != nil {
				glog.V(0).Infof("failed to parse master grpc %v: %v", masterGrpcAddress, parseErr)
				continue
			}
			vs.store.MasterAddress = master
			newLeader, err = vs.doHeartbeat(master, masterGrpcAddress, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
				newLeader = ""
				vs.store.MasterAddress = ""
			}
		}
	}
}

func (vs *VolumeServer) doHeartbeat(masterNode, masterGrpcAddress string, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader string, err error) {

	grpcConection, err := pb.GrpcDial(context.Background(), masterGrpcAddress, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterNode, err)
	}
	defer grpcConection.Close()

	client := master_pb.NewSeaweedClient(grpcConection)
	/// 获取 seaweedSendHeartbeatClient
	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterNode, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to: %v", masterNode)
	vs.currentMaster = masterNode

	doneChan := make(chan error, 1)

	/// 这里是从 master 接收 响应, 以接收更改的参数
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			/// 如果单个 volume 的 大小限制改变了,就需要重新计算 能支持的 最大 volume 数目
			if in.GetVolumeSizeLimit() != 0 && vs.store.GetVolumeSizeLimit() != in.GetVolumeSizeLimit() {
				vs.store.SetVolumeSizeLimit(in.GetVolumeSizeLimit())
				/// 计算 该 节点 的 最大可支持 volume 数目 MaxVolumeCount
				/// 根据 硬盘 剩余空间大小, 为每个 volume 预留 出足够大的空间, 然后计算还可以增加多少个 volume
				if vs.store.MaybeAdjustVolumeMax() {
					/// 回收 本地过期的 volume
					/// 统计各个 DiskLocation 中的 volume 总数, 最大的文件 key id, .dat 文件大小, FileCount DeleteCount DeletedSize 这类指标数据
					if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
						glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
					}
				}
			}
			/// 如果不是 leader master 就退出, 也就是说只向 leader master 发送信息
			/// 如果 master 改变了怎么办, 各个 master 之间数据一致性怎么保证
			if in.GetLeader() != "" && masterNode != in.GetLeader() && !isSameIP(in.GetLeader(), masterNode) {
				glog.V(0).Infof("Volume Server found a new master newLeader: %v instead of %v", in.GetLeader(), masterNode)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
			/// 修改 metrics 地址 信息
			if in.GetMetricsAddress() != "" && vs.MetricsAddress != in.GetMetricsAddress() {
				vs.MetricsAddress = in.GetMetricsAddress()
				vs.MetricsIntervalSec = int(in.GetMetricsIntervalSeconds())
			}
			if len(in.StorageBackends) > 0 {
				backend.LoadFromPbStorageBackends(in.StorageBackends)
			}
		}
	}()

	/// 然后发送 心跳 数据包, 发送的内容见上面
	/// CollectHeartbeat中还有个功能就是回收本地过期的 volume
	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	volumeTickChan := time.Tick(sleepInterval)
	ecShardTickChan := time.Tick(17 * sleepInterval)

	for {
		select {
		/// 增加卷 和 挂载卷 的时候会向该 channel 中写入信息, 里面包含 Id, Collection, ReplicaPlacement, Version, Ttl 信息
		case volumeMessage := <-vs.store.NewVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				NewVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.NewEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		/// 解挂 和 删除 卷 的时候 会向该 通道 发送消息, 里面包含 Id, Collection, ReplicaPlacement, Version, Ttl 信息
		case volumeMessage := <-vs.store.DeletedVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.DeletedEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		/// 间隔 发送 心跳 信息, 发送的内容见上面
		case <-volumeTickChan:
			glog.V(4).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case <-ecShardTickChan:
			glog.V(4).Infof("volume server %s:%d ec heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case err = <-doneChan:
			return
		}
	}
}

func isSameIP(ip string, host string) bool {
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, t := range ips {
		if ip == t.String() {
			return true
		}
	}
	return false
}
