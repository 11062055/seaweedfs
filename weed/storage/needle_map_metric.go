package storage

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/willf/bloom"
)

/// needle map 的 本地 metrics
type mapMetric struct {
	DeletionCounter     uint32 `json:"DeletionCounter"`
	FileCounter         uint32 `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	if mm == nil {
		return
	}
	mm.LogDeletionCounter(deletedByteCount)
}

func (mm *mapMetric) logPut(key NeedleId, oldSize uint32, newSize uint32) {
	if mm == nil {
		return
	}
	mm.MaybeSetMaxFileKey(key)
	mm.LogFileCounter(newSize)
	if oldSize > 0 && oldSize != TombstoneFileSize {
		mm.LogDeletionCounter(oldSize)
	}
}
func (mm *mapMetric) LogFileCounter(newSize uint32) {
	if mm == nil {
		return
	}
	atomic.AddUint32(&mm.FileCounter, 1)
	atomic.AddUint64(&mm.FileByteCounter, uint64(newSize))
}
func (mm *mapMetric) LogDeletionCounter(oldSize uint32) {
	if mm == nil {
		return
	}
	if oldSize > 0 {
		atomic.AddUint32(&mm.DeletionCounter, 1)
		atomic.AddUint64(&mm.DeletionByteCounter, uint64(oldSize))
	}
}
func (mm *mapMetric) ContentSize() uint64 {
	if mm == nil {
		return 0
	}
	return atomic.LoadUint64(&mm.FileByteCounter)
}
func (mm *mapMetric) DeletedSize() uint64 {
	if mm == nil {
		return 0
	}
	return atomic.LoadUint64(&mm.DeletionByteCounter)
}
func (mm *mapMetric) FileCount() int {
	if mm == nil {
		return 0
	}
	return int(atomic.LoadUint32(&mm.FileCounter))
}
func (mm *mapMetric) DeletedCount() int {
	if mm == nil {
		return 0
	}
	return int(atomic.LoadUint32(&mm.DeletionCounter))
}
func (mm *mapMetric) MaxFileKey() NeedleId {
	if mm == nil {
		return 0
	}
	t := uint64(mm.MaximumFileKey)
	return Uint64ToNeedleId(t)
}
func (mm *mapMetric) MaybeSetMaxFileKey(key NeedleId) {
	if mm == nil {
		return
	}
	if key > mm.MaxFileKey() {
		atomic.StoreUint64(&mm.MaximumFileKey, uint64(key))
	}
}

/// 将 .idx 中的文件 的相关 数据 遍历 到 metric 中去, 第一个 函数用于 布隆过滤器 的初始化
/// 读取 最大 的 file key, 统计 所有文件总的 大小, 删除 文件 总的 次数, 删除 文件 总的 大小
func newNeedleMapMetricFromIndexFile(r *os.File) (mm *mapMetric, err error) {
	mm = &mapMetric{}
	var bf *bloom.BloomFilter
	buf := make([]byte, NeedleIdSize)
	/// 遍历 .idx 文件, 并且针对每条 数据 调用 函数 fn
	err = reverseWalkIndexFile(r, func(entryCount int64) {
		bf = bloom.NewWithEstimates(uint(entryCount), 0.001)
	}, func(key NeedleId, offset Offset, size uint32) error {

		mm.MaybeSetMaxFileKey(key)
		NeedleIdToBytes(buf, key)
		if size != TombstoneFileSize {
			mm.FileByteCounter += uint64(size)
		}

		if !bf.Test(buf) {
			mm.FileCounter++
			bf.Add(buf)
		} else {
			// deleted file
			mm.DeletionCounter++
			if size != TombstoneFileSize {
				// previously already deleted file
				mm.DeletionByteCounter += uint64(size)
			}
		}
		return nil
	})
	return
}

/// 遍历 .idx 文件, 并且针对每条 数据 调用 函数 fn
func reverseWalkIndexFile(r *os.File, initFn func(entryCount int64), fn func(key NeedleId, offset Offset, size uint32) error) error {
	fi, err := r.Stat()
	if err != nil {
		return fmt.Errorf("file %s stat error: %v", r.Name(), err)
	}
	fileSize := fi.Size()
	if fileSize%NeedleMapEntrySize != 0 {
		return fmt.Errorf("unexpected file %s size: %d", r.Name(), fileSize)
	}

	entryCount := fileSize / NeedleMapEntrySize
	initFn(entryCount)

	batchSize := int64(1024 * 4)

	bytes := make([]byte, NeedleMapEntrySize*batchSize)
	nextBatchSize := entryCount % batchSize
	if nextBatchSize == 0 {
		nextBatchSize = batchSize
	}
	remainingCount := entryCount - nextBatchSize

	for remainingCount >= 0 {
		_, e := r.ReadAt(bytes[:NeedleMapEntrySize*nextBatchSize], NeedleMapEntrySize*remainingCount)
		// glog.V(0).Infoln("file", r.Name(), "readerOffset", NeedleMapEntrySize*remainingCount, "count", count, "e", e)
		if e != nil {
			return e
		}
		for i := int(nextBatchSize) - 1; i >= 0; i-- {
			key, offset, size := idx.IdxFileEntry(bytes[i*NeedleMapEntrySize : i*NeedleMapEntrySize+NeedleMapEntrySize])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		nextBatchSize = batchSize
		remainingCount -= nextBatchSize
	}
	return nil
}
