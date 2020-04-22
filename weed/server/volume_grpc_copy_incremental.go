package weed_server

import (
	"context"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 读取 某一 纳秒 后的所有数据 即增量读取
func (vs *VolumeServer) VolumeIncrementalCopy(req *volume_server_pb.VolumeIncrementalCopyRequest, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	stopOffset, _, _ := v.FileStat()
	/// 从 sinceNs 纳秒 处 开始, 通过 index file 读取 1 个 needle 在 volume 的 offset
	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(req.SinceNs)
	if err != nil {
		return fmt.Errorf("fail to locate by appendAtNs %d: %s", req.SinceNs, err)
	}

	if isLastOne {
		return nil
	}

	startOffset := foundOffset.ToAcutalOffset()

	buf := make([]byte, 1024*1024*2)
	/// 从 既定 偏移量 处 开始读取 数据 每次读取buf大小的数据 并且发送
	return sendFileContent(v.DataBackend, buf, startOffset, int64(stopOffset), stream)

}

func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp := v.GetVolumeSyncStatus()

	return resp, nil

}

/// 从 既定 偏移量 处 开始读取 数据 每次读取buf大小的数据 并且发送
func sendFileContent(datBackend backend.BackendStorageFile, buf []byte, startOffset, stopOffset int64, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {
	var blockSizeLimit = int64(len(buf))
	for i := int64(0); i < stopOffset-startOffset; i += blockSizeLimit {
		n, readErr := datBackend.ReadAt(buf, startOffset+i)
		if readErr == nil || readErr == io.EOF {
			resp := &volume_server_pb.VolumeIncrementalCopyResponse{}
			resp.FileContent = buf[:int64(n)]
			sendErr := stream.Send(resp)
			if sendErr != nil {
				return sendErr
			}
		} else {
			return readErr
		}
	}
	return nil
}
