package weed_server

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 检测垃圾率
func (vs *VolumeServer) VacuumVolumeCheck(ctx context.Context, req *volume_server_pb.VacuumVolumeCheckRequest) (*volume_server_pb.VacuumVolumeCheckResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCheckResponse{}

	garbageRatio, err := vs.store.CheckCompactVolume(needle.VolumeId(req.VolumeId))

	resp.GarbageRatio = garbageRatio

	if err != nil {
		glog.V(3).Infof("check volume %d: %v", req.VolumeId, err)
	}

	return resp, err

}

/// 压缩空洞 实际是使用复制算法 进行文件腾挪  进行空洞去除  然后删除临时文件
func (vs *VolumeServer) VacuumVolumeCompact(ctx context.Context, req *volume_server_pb.VacuumVolumeCompactRequest) (*volume_server_pb.VacuumVolumeCompactResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCompactResponse{}

	err := vs.store.CompactVolume(needle.VolumeId(req.VolumeId), req.Preallocate, vs.compactionBytePerSecond)

	if err != nil {
		glog.Errorf("compact volume %d: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("compact volume %d", req.VolumeId)
	}

	return resp, err

}

/// 提交压缩 实际是进行 临时复制拷贝 文件的 rename
func (vs *VolumeServer) VacuumVolumeCommit(ctx context.Context, req *volume_server_pb.VacuumVolumeCommitRequest) (*volume_server_pb.VacuumVolumeCommitResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCommitResponse{}

	err := vs.store.CommitCompactVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("commit volume %d: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("commit volume %d", req.VolumeId)
	}
	if err == nil {
		if vs.store.GetVolume(needle.VolumeId(req.VolumeId)).IsReadOnly() {
			resp.IsReadOnly = true
		}
	}

	return resp, err

}

/// 删除临时拷贝文件
func (vs *VolumeServer) VacuumVolumeCleanup(ctx context.Context, req *volume_server_pb.VacuumVolumeCleanupRequest) (*volume_server_pb.VacuumVolumeCleanupResponse, error) {

	resp := &volume_server_pb.VacuumVolumeCleanupResponse{}

	err := vs.store.CommitCleanupVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("cleanup volume %d: %v", req.VolumeId, err)
	} else {
		glog.V(1).Infof("cleanup volume %d", req.VolumeId)
	}

	return resp, err

}
