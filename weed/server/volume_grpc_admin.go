package weed_server

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

/// 响应 leader master 的 DeleteCollection 请求
func (vs *VolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {

	resp := &volume_server_pb.DeleteCollectionResponse{}

	/// 调用 store 的 DeleteCollection, 其中会调用 DiskLocation 的 DeleteCollectionFromDiskLocation
	/// 触发调用 Volume 的 Destroy 和 EcVolume 的 Destroy 从硬盘中删除数据
	err := vs.store.DeleteCollection(req.Collection)

	if err != nil {
		glog.Errorf("delete collection %s: %v", req.Collection, err)
	} else {
		glog.V(2).Infof("delete collection %v", req)
	}

	return resp, err

}

/// 申请 volume
func (vs *VolumeServer) AllocateVolume(ctx context.Context, req *volume_server_pb.AllocateVolumeRequest) (*volume_server_pb.AllocateVolumeResponse, error) {

	resp := &volume_server_pb.AllocateVolumeResponse{}

	err := vs.store.AddVolume(
		needle.VolumeId(req.VolumeId),
		req.Collection,
		vs.needleMapKind,
		req.Replication,
		req.Ttl,
		req.Preallocate,
		req.MemoryMapMaxSizeMb,
	)

	if err != nil {
		glog.Errorf("assign volume %v: %v", req, err)
	} else {
		glog.V(2).Infof("assign volume %v", req)
	}

	return resp, err

}

/// 递归加载每个子目录中的卷信息
/// Store->DiskLocation->Volume->Needle
func (vs *VolumeServer) VolumeMount(ctx context.Context, req *volume_server_pb.VolumeMountRequest) (*volume_server_pb.VolumeMountResponse, error) {

	resp := &volume_server_pb.VolumeMountResponse{}

	err := vs.store.MountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mount %v", req)
	}

	return resp, err

}

/// 解挂卷 从 DiskLocation 的 卷列表中去除 但是不删除物理卷
func (vs *VolumeServer) VolumeUnmount(ctx context.Context, req *volume_server_pb.VolumeUnmountRequest) (*volume_server_pb.VolumeUnmountResponse, error) {

	resp := &volume_server_pb.VolumeUnmountResponse{}

	err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume unmount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume unmount %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {

	resp := &volume_server_pb.VolumeDeleteResponse{}

	/// 物理删除 并从 DiskLocation 的卷列表中删除
	err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume delete %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume delete %v", req)
	}

	return resp, err

}

/// 配置 volume 卷
func (vs *VolumeServer) VolumeConfigure(ctx context.Context, req *volume_server_pb.VolumeConfigureRequest) (*volume_server_pb.VolumeConfigureResponse, error) {

	resp := &volume_server_pb.VolumeConfigureResponse{}

	// check replication format
	if _, err := super_block.NewReplicaPlacementFromString(req.Replication); err != nil {
		resp.Error = fmt.Sprintf("volume configure replication %v: %v", req, err)
		return resp, nil
	}

	// unmount
	/// 首先 需要 解挂
	if err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure unmount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure unmount %v: %v", req, err)
		return resp, nil
	}

	// modify the volume info file
	/// 然后配置
	if err := vs.store.ConfigureVolume(needle.VolumeId(req.VolumeId), req.Replication); err != nil {
		glog.Errorf("volume configure %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure %v: %v", req, err)
		return resp, nil
	}

	// mount
	/// 再挂载
	if err := vs.store.MountVolume(needle.VolumeId(req.VolumeId)); err != nil {
		glog.Errorf("volume configure mount %v: %v", req, err)
		resp.Error = fmt.Sprintf("volume configure mount %v: %v", req, err)
		return resp, nil
	}

	return resp, nil

}

/// 标记为只读
func (vs *VolumeServer) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {

	resp := &volume_server_pb.VolumeMarkReadonlyResponse{}

	err := vs.store.MarkVolumeReadonly(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mark readonly %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark readonly %v", req)
	}

	return resp, err

}

/// 获取 volume server 的状态
func (vs *VolumeServer) VolumeServerStatus(ctx context.Context, req *volume_server_pb.VolumeServerStatusRequest) (*volume_server_pb.VolumeServerStatusResponse, error) {

	resp := &volume_server_pb.VolumeServerStatusResponse{}

	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			resp.DiskStatuses = append(resp.DiskStatuses, stats.NewDiskStatus(dir))
		}
	}

	resp.MemoryStatus = stats.MemStat()

	return resp, nil

}
