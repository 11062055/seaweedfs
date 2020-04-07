package storage

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 检测 垃圾率
func (s *Store) CheckCompactVolume(volumeId needle.VolumeId) (float64, error) {
	if v := s.findVolume(volumeId); v != nil {
		glog.V(3).Infof("volumd %d garbage level: %f", volumeId, v.garbageLevel())
		return v.garbageLevel(), nil
	}
	return 0, fmt.Errorf("volume id %d is not found during check compact", volumeId)
}
/// 压缩空洞 实际是进行文件腾挪  进行空洞去除  然后删除临时文件
func (s *Store) CompactVolume(vid needle.VolumeId, preallocate int64, compactionBytePerSecond int64) error {
	if v := s.findVolume(vid); v != nil {
		return v.Compact2(preallocate, compactionBytePerSecond)
	}
	return fmt.Errorf("volume id %d is not found during compact", vid)
}
/// 提交压缩 实际是进行 临时拷贝 文件的 rename
func (s *Store) CommitCompactVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.CommitCompact()
	}
	return fmt.Errorf("volume id %d is not found during commit compact", vid)
}
/// 删除临时拷贝文件
func (s *Store) CommitCleanupVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up", vid)
}
