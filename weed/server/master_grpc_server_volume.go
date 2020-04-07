package weed_server

import (
	"context"
	"fmt"

	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

/// 根据 volume id 获取卷的 location 信息, 只有 leader master 才响应请求
func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupVolumeResponse{}
	/// 获取各个 volume id 的 location 信息, 从本地 map 中获取, 会通过 Volume 和 VolumeLayout 查找
	volumeLocations := ms.lookupVolumeId(req.VolumeIds, req.Collection)

	for _, result := range volumeLocations {
		var locations []*master_pb.Location
		for _, loc := range result.Locations {
			locations = append(locations, &master_pb.Location{
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
			})
		}
		/// 每个 volume id 对应很多 location
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeId:  result.VolumeId,
			Locations: locations,
			Error:     result.Error,
		})
	}

	return resp, nil
}

/// 分配 fid 和 url, 只有 master 才响应请求
func (ms *MasterServer) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Count == 0 {
		req.Count = 1
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		Prealloacte:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
	}

	/// 查找是否有可写的 卷, 通过 查找 volume layout 的内存可写列表进行查找
	if !ms.Topo.HasWritableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			return nil, fmt.Errorf("No free volumes left!")
		}
		ms.vgLock.Lock()
		if !ms.Topo.HasWritableVolume(option) {
			/// 扩容, 先在本地查找是否有满足要求的节点，如果有就向目标 volume server 节点发起 AllocateVolume 请求
			if _, err = ms.vg.AutomaticGrowByType(option, ms.grpcDialOption, ms.Topo, int(req.WritableVolumeCount)); err != nil {
				ms.vgLock.Unlock()
				return nil, fmt.Errorf("Cannot grow volume group! %v", err)
			}
		}
		ms.vgLock.Unlock()
	}
	/// 调用 topo 的PickForWrite, 它会递归调用 VolumeLayout 的 PickForWrite 查找一个符合要求的节点, 本地操作
	fid, count, dn, err := ms.Topo.PickForWrite(req.Count, option)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	return &master_pb.AssignResponse{
		Fid:       fid,
		Url:       dn.Url(),
		PublicUrl: dn.PublicUrl,
		Count:     count,
		Auth:      string(security.GenJwt(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fid)),
	}, nil
}

/// 获取统计信息, 本地操作
func (ms *MasterServer) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl)
	/// 调用 VolumeLayout 的 Stats 函数
	stats := volumeLayout.Stats()

	totalSize := ms.Topo.GetMaxVolumeCount() * int64(ms.option.VolumeSizeLimitMB) * 1024 * 1024

	resp := &master_pb.StatisticsResponse{
		TotalSize: uint64(totalSize),
		UsedSize:  stats.UsedSize,
		FileCount: stats.FileCount,
	}

	return resp, nil
}

/// 获取 volume list, 只有 leader master 才执行, 调用 topology 的 ToTopologyInfo 方法
func (ms *MasterServer) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	/// 调用 ToTopologyInfo 方法, 会递归调用 DataCenter 的 ToDataCenterInfo(), 其中递归调用 rack 和 data node 的相关方法获取整个拓扑信息
	resp := &master_pb.VolumeListResponse{
		TopologyInfo:      ms.Topo.ToTopologyInfo(),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}

	return resp, nil
}

/// 获取 ec volume, 只有 leader master 才执行
func (ms *MasterServer) LookupEcVolume(ctx context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupEcVolumeResponse{}

	/// LookupEcShards 直接从 topology 中 ecShardMap 中获取数据
	ecLocations, found := ms.Topo.LookupEcShards(needle.VolumeId(req.VolumeId))

	if !found {
		return resp, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId

	for shardId, shardLocations := range ecLocations.Locations {
		var locations []*master_pb.Location
		for _, dn := range shardLocations {
			locations = append(locations, &master_pb.Location{
				Url:       string(dn.Id()),
				PublicUrl: dn.PublicUrl,
			})
		}
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: locations,
		})
	}

	return resp, nil
}

func (ms *MasterServer) GetMasterConfiguration(ctx context.Context, req *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {

	resp := &master_pb.GetMasterConfigurationResponse{
		MetricsAddress:         ms.option.MetricsAddress,
		MetricsIntervalSeconds: uint32(ms.option.MetricsIntervalSec),
	}

	return resp, nil
}
