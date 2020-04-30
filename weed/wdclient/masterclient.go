package wdclient

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MasterClient struct {
	clientType     string
	clientHost     string
	grpcPort       uint32
	currentMaster  string
	masters        []string
	grpcDialOption grpc.DialOption

	vidMap
}

/// master 之间相互通信 的 client
func NewMasterClient(grpcDialOption grpc.DialOption, clientType string, clientHost string, clientGrpcPort uint32, masters []string) *MasterClient {
	return &MasterClient{
		clientType:     clientType,
		clientHost:     clientHost,
		grpcPort:       clientGrpcPort,
		masters:        masters,
		grpcDialOption: grpcDialOption,
		vidMap:         newVidMap(),
	}
}

func (mc *MasterClient) GetMaster() string {
	return mc.currentMaster
}

func (mc *MasterClient) WaitUntilConnected() {
	for mc.currentMaster == "" {
		time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
	}
}

/// master 之间保持连接 循环 更新 volume id 到 location 之间的对应关系, 数据保存在 master client 的 vidMap 中的
func (mc *MasterClient) KeepConnectedToMaster() {
	glog.V(1).Infof("%s bootstraps with masters %v", mc.clientType, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

/// 连接到 master 循环 更新 volume id 到 location 之间的对应关系
/// 实际是通过 KeepConnected 发送请求 到 leader master, 并且从 leader master 处循环接收 每个 节点的 volume id 变动信息
/// 而 leader master 是通过 接收 volume server 的 SendHeartbeat 请求 得知 volume id 的 变更信息的
func (mc *MasterClient) tryAllMasters() {
	nextHintedLeader := ""
	for _, master := range mc.masters {

		nextHintedLeader = mc.tryConnectToMaster(master)
		/// leader master 有变动
		for nextHintedLeader != "" {
			nextHintedLeader = mc.tryConnectToMaster(nextHintedLeader)
		}

		mc.currentMaster = ""
		mc.vidMap = newVidMap()
	}
}

/// 连接到 master 循环更新 volume id 到 location 的 映射
func (mc *MasterClient) tryConnectToMaster(master string) (nextHintedLeader string) {
	glog.V(1).Infof("%s Connecting to master %v", mc.clientType, master)
	gprcErr := pb.WithMasterClient(master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {

		/// stream 是 seaweedKeepConnectedClient
		/// 向 leader master 发起请求, 接收 哪些 节点 有哪些 volume 变动
		/// 每个 volume server 会向 leader master 发送 heart beat 以 报告它所处节点有哪些 volume 变更了
		stream, err := client.KeepConnected(context.Background())
		if err != nil {
			glog.V(0).Infof("%s failed to keep connected to %s: %v", mc.clientType, master, err)
			return err
		}

		if err = stream.Send(&master_pb.KeepConnectedRequest{Name: mc.clientType, GrpcPort: mc.grpcPort}); err != nil {
			glog.V(0).Infof("%s failed to send to %s: %v", mc.clientType, master, err)
			return err
		}

		glog.V(1).Infof("%s Connected to %v", mc.clientType, master)
		mc.currentMaster = master

		for {
			volumeLocation, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s failed to receive from %s: %v", mc.clientType, master, err)
				return err
			}

			// maybe the leader is changed
			/// 由于只向 leader master 发起 KeepConnected 请求, 当对方不是leader的时候, 对方 master 会从 topology 中的 raft server 获取 leader 并 返回
			if volumeLocation.Leader != "" {
				glog.V(0).Infof("redirected to leader %v", volumeLocation.Leader)
				nextHintedLeader = volumeLocation.Leader
				return nil
			}

			/// 记录 leader master 中收到和保存到的的 volume id 和 对应的 位置 location url
			// process new volume location
			loc := Location{
				Url:       volumeLocation.Url,
				PublicUrl: volumeLocation.PublicUrl,
			}
			/// 把新增和删除的 volume 数据 保存到 本地 内存中
			for _, newVid := range volumeLocation.NewVids {
				glog.V(1).Infof("%s: %s adds volume %d", mc.clientType, loc.Url, newVid)
				mc.addLocation(newVid, loc)
			}
			for _, deletedVid := range volumeLocation.DeletedVids {
				glog.V(1).Infof("%s: %s removes volume %d", mc.clientType, loc.Url, deletedVid)
				mc.deleteLocation(deletedVid, loc)
			}
		}

	})
	if gprcErr != nil {
		glog.V(0).Infof("%s failed to connect with master %v: %v", mc.clientType, master, gprcErr)
	}
	return
}

func (mc *MasterClient) WithClient(fn func(client master_pb.SeaweedClient) error) error {
	return pb.WithMasterClient(mc.currentMaster, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})
}
