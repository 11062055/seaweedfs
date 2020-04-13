package weed_server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/chrislusf/raft"
	"google.golang.org/grpc/peer"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

/// 当收到 心跳后 需要确认是否 新增 data center 或者 rack 或者 data node
func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	t := ms.Topo

	defer func() {
		if dn != nil {

			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)
			t.UnRegisterDataNode(dn)

			message := &master_pb.VolumeLocation{
				Url:       dn.Url(),
				PublicUrl: dn.PublicUrl,
			}
			for _, v := range dn.GetVolumes() {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
			for _, s := range dn.GetEcShards() {
				message.DeletedVids = append(message.DeletedVids, uint32(s.VolumeId))
			}

			if len(message.DeletedVids) > 0 {
				ms.clientChansLock.RLock()
				for _, ch := range ms.clientChans {
					ch <- message
				}
				ms.clientChansLock.RUnlock()
			}

		}
	}()

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			if dn != nil {
				glog.Warningf("SendHeartbeat.Recv server %s:%d : %v", dn.Ip, dn.Port, err)
			} else {
				glog.Warningf("SendHeartbeat.Recv: %v", err)
			}
			return err
		}

		t.Sequence.SetMax(heartbeat.MaxFileKey)

		if dn == nil {
			/// 从配置文件获取
			dcName, rackName := t.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
			/// 获取已有 或者 创建 data center \ rack \ data node
			dc := t.GetOrCreateDataCenter(dcName)
			rack := dc.GetOrCreateRack(rackName)
			dn = rack.GetOrCreateDataNode(heartbeat.Ip,
				int(heartbeat.Port), heartbeat.PublicUrl,
				int64(heartbeat.MaxVolumeCount))
			glog.V(0).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
			if err := stream.Send(&master_pb.HeartbeatResponse{
				VolumeSizeLimit:        uint64(ms.option.VolumeSizeLimitMB) * 1024 * 1024,
				MetricsAddress:         ms.option.MetricsAddress,
				MetricsIntervalSeconds: uint32(ms.option.MetricsIntervalSec),
				StorageBackends:        backend.ToPbStorageBackends(),
			}); err != nil {
				glog.Warningf("SendHeartbeat.Send volume size to %s:%d %v", dn.Ip, dn.Port, err)
				return err
			}
		}

		if heartbeat.MaxVolumeCount != 0 && dn.GetMaxVolumeCount() != int64(heartbeat.MaxVolumeCount) {
			delta := int64(heartbeat.MaxVolumeCount) - dn.GetMaxVolumeCount()
			dn.UpAdjustMaxVolumeCountDelta(delta)
		}

		glog.V(4).Infof("master received heartbeat %s", heartbeat.String())
		message := &master_pb.VolumeLocation{
			Url:       dn.Url(),
			PublicUrl: dn.PublicUrl,
		}
		if len(heartbeat.NewVolumes) > 0 || len(heartbeat.DeletedVolumes) > 0 {
			// process delta volume ids if exists for fast volume id updates
			for _, volInfo := range heartbeat.NewVolumes {
				message.NewVids = append(message.NewVids, volInfo.Id)
			}
			for _, volInfo := range heartbeat.DeletedVolumes {
				message.DeletedVids = append(message.DeletedVids, volInfo.Id)
			}
			// update master internal volume layouts
			/// 增量通知 topology 和 node 更新 volume 信息
			t.IncrementalSyncDataNodeRegistration(heartbeat.NewVolumes, heartbeat.DeletedVolumes, dn)
		}

		if len(heartbeat.Volumes) > 0 || heartbeat.HasNoVolumes {
			// process heartbeat.Volumes
			/// 更新节点 和 该节点的 volume 信息
			newVolumes, deletedVolumes := t.SyncDataNodeRegistration(heartbeat.Volumes, dn)

			for _, v := range newVolumes {
				glog.V(0).Infof("master see new volume %d from %s", uint32(v.Id), dn.Url())
				message.NewVids = append(message.NewVids, uint32(v.Id))
			}
			for _, v := range deletedVolumes {
				glog.V(0).Infof("master see deleted volume %d from %s", uint32(v.Id), dn.Url())
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
		}

		if len(heartbeat.NewEcShards) > 0 || len(heartbeat.DeletedEcShards) > 0 {

			// update master internal volume layouts
			/// 更新 topology 和 对应的 data node 的 ec shard 信息
			t.IncrementalSyncDataNodeEcShards(heartbeat.NewEcShards, heartbeat.DeletedEcShards, dn)

			for _, s := range heartbeat.NewEcShards {
				message.NewVids = append(message.NewVids, s.Id)
			}
			for _, s := range heartbeat.DeletedEcShards {
				if dn.HasVolumesById(needle.VolumeId(s.Id)) {
					continue
				}
				message.DeletedVids = append(message.DeletedVids, s.Id)
			}

		}

		if len(heartbeat.EcShards) > 0 || heartbeat.HasNoEcShards {
			glog.V(1).Infof("master recieved ec shards from %s: %+v", dn.Url(), heartbeat.EcShards)
			/// 更新 topology 中的 ec shard 信息 和 data node 中的 ec shard 反向索引信息
			newShards, deletedShards := t.SyncDataNodeEcShards(heartbeat.EcShards, dn)

			// broadcast the ec vid changes to master clients
			for _, s := range newShards {
				message.NewVids = append(message.NewVids, uint32(s.VolumeId))
			}
			for _, s := range deletedShards {
				if dn.HasVolumesById(s.VolumeId) {
					continue
				}
				message.DeletedVids = append(message.DeletedVids, uint32(s.VolumeId))
			}

		}

		/// 发送消息个所有的 channel, 这样所有消息都能在所有 master 之间传递了
		if len(message.NewVids) > 0 || len(message.DeletedVids) > 0 {
			ms.clientChansLock.RLock()
			for host, ch := range ms.clientChans {
				glog.V(0).Infof("master send to %s: %s", host, message.String())
				ch <- message
			}
			ms.clientChansLock.RUnlock()
		}

		// tell the volume servers about the leader
		newLeader, err := t.Leader()
		if err != nil {
			glog.Warningf("SendHeartbeat find leader: %v", err)
			return err
		}
		if err := stream.Send(&master_pb.HeartbeatResponse{
			Leader: newLeader,
		}); err != nil {
			glog.Warningf("SendHeartbeat.Send response to to %s:%d %v", dn.Ip, dn.Port, err)
			return err
		}
	}
}

// KeepConnected keep a stream gRPC call to the master. Used by clients to know the master is up.
// And clients gets the up-to-date list of volume locations

/// master 之间 grpc 调用, 处理 KeepConnected 命令, 每次收到 volume server 的 SendHeartbeat 命令时 该函数都会向 master 客户端写回 volume info
/// 也就说 volume server 向 master 发送 SendHeartbeat 命令, master 之间通过 KeepConnected 实现消息的传递和共享
func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	/// 如果不是 leader 则从 leader 处获取信息
	if !ms.Topo.IsLeader() {
		return ms.informNewLeader(stream)
	}

	peerAddress := findClientAddress(stream.Context(), req.GrpcPort)

	// only one shell can be connected at any time
	if req.Name == pb.AdminShellClient {
		if ms.currentAdminShellClient == "" {
			ms.currentAdminShellClient = peerAddress
			defer func() {
				ms.currentAdminShellClient = ""
			}()
		} else {
			return fmt.Errorf("only one concurrent shell allowed, but another shell is already connected from %s", peerAddress)
		}
	}

	stopChan := make(chan bool)

	/// messageChan = make(chan *master_pb.VolumeLocation) 是在 SendHeartbeat 中写入的
	clientName, messageChan := ms.addClient(req.Name, peerAddress)

	defer ms.deleteClient(clientName)

	/// 获取 volume 信息
	for _, message := range ms.Topo.ToVolumeLocations() {
		if err := stream.Send(message); err != nil {
			return err
		}
	}

	/// 检查 client 是否 stop
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				glog.V(2).Infof("- client %v: %v", clientName, err)
				stopChan <- true
				break
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		/// 所有 volume data node 节点 的状态变化会通过 SendHeartbeat 发送给一个 master,
		/// 一旦有变化会发送给 master 的所有 messageChan
		/// master 之间通过 KeepConnected 相互获取状态变化, 并从 messageChan 中相互交换变化的信息
		case message := <-messageChan:
			if err := stream.Send(message); err != nil {
				glog.V(0).Infof("=> client %v: %+v", clientName, message)
				return err
			}
			/// 期间 leader 可能会改变
		case <-ticker.C:
			if !ms.Topo.IsLeader() {
				return ms.informNewLeader(stream)
			}
		case <-stopChan:
			return nil
		}
	}

	return nil
}

func (ms *MasterServer) informNewLeader(stream master_pb.Seaweed_KeepConnectedServer) error {
	leader, err := ms.Topo.Leader()
	if err != nil {
		glog.Errorf("topo leader: %v", err)
		return raft.NotLeaderError
	}
	if err := stream.Send(&master_pb.VolumeLocation{
		Leader: leader,
	}); err != nil {
		return err
	}
	return nil
}

/// 增加一个 master client, 并设置一个 channel, 用于移步接收和通知该 client 相关信息, SendHeartbeat 和 KeepConnected 中在使用
func (ms *MasterServer) addClient(clientType string, clientAddress string) (clientName string, messageChan chan *master_pb.VolumeLocation) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ client %v", clientName)

	messageChan = make(chan *master_pb.VolumeLocation)

	ms.clientChansLock.Lock()
	ms.clientChans[clientName] = messageChan
	ms.clientChansLock.Unlock()
	return
}

func (ms *MasterServer) deleteClient(clientName string) {
	glog.V(0).Infof("- client %v", clientName)
	ms.clientChansLock.Lock()
	delete(ms.clientChans, clientName)
	ms.clientChansLock.Unlock()
}

func findClientAddress(ctx context.Context, grpcPort uint32) string {
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return ""
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return ""
	}
	if grpcPort == 0 {
		return pr.Addr.String()
	}
	if tcpAddr, ok := pr.Addr.(*net.TCPAddr); ok {
		externalIP := tcpAddr.IP
		return fmt.Sprintf("%s:%d", externalIP, grpcPort)
	}
	return pr.Addr.String()

}

/// 获取连接的 master client( 其本身 也是 master )
func (ms *MasterServer) ListMasterClients(ctx context.Context, req *master_pb.ListMasterClientsRequest) (*master_pb.ListMasterClientsResponse, error) {
	resp := &master_pb.ListMasterClientsResponse{}
	ms.clientChansLock.RLock()
	defer ms.clientChansLock.RUnlock()

	/// clientChans     map[string]chan *master_pb.VolumeLocation
	for k := range ms.clientChans {
		if strings.HasPrefix(k, req.ClientType+"@") {
			resp.GrpcAddresses = append(resp.GrpcAddresses, k[len(req.ClientType)+1:])
		}
	}
	return resp, nil
}
