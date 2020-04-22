package needle_map

import (
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

/// 接口, 可以自己实现, 比如 基于 leveldb 的 NewMemDb
type NeedleValueMap interface {
	Set(key NeedleId, offset Offset, size uint32) (oldOffset Offset, oldSize uint32)
	Delete(key NeedleId) uint32
	Get(key NeedleId) (*NeedleValue, bool)
	AscendingVisit(visit func(NeedleValue) error) error
}
