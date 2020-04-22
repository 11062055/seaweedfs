package chunk_cache

import (
	"fmt"
	"os"
	"time"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// This implements an on disk cache
// The entries are an FIFO with a size limit

type ChunkCacheVolume struct {
	DataBackend backend.BackendStorageFile
	nm          storage.NeedleMapper
	fileName    string
	smallBuffer []byte
	sizeLimit   int64
	lastModTime time.Time
	fileSize    int64
}

/// 本地 磁盘 缓存, leveldb 中存储 .idx 文件中的索引数据, 而 .dat 文件中存储实际数据
func LoadOrCreateChunkCacheVolume(fileName string, preallocate int64) (*ChunkCacheVolume, error) {

	v := &ChunkCacheVolume{
		smallBuffer: make([]byte, types.NeedlePaddingSize),
		fileName:    fileName,
		sizeLimit:   preallocate,
	}

	var err error

	if exists, canRead, canWrite, modTime, fileSize := util.CheckFile(v.fileName + ".dat"); exists {
		if !canRead {
			return nil, fmt.Errorf("cannot read cache file %s.dat", v.fileName)
		}
		if !canWrite {
			return nil, fmt.Errorf("cannot write cache file %s.dat", v.fileName)
		}
		if dataFile, err := os.OpenFile(v.fileName+".dat", os.O_RDWR|os.O_CREATE, 0644); err != nil {
			return nil, fmt.Errorf("cannot create cache file %s.dat: %v", v.fileName, err)
		} else {
			/// 磁盘文件 当作 缓存
			v.DataBackend = backend.NewDiskFile(dataFile)
			v.lastModTime = modTime
			v.fileSize = fileSize
		}
	} else {
		/// 创建文件, 同时调用 NewDiskFile 使用 磁盘文件 当作 缓存
		if v.DataBackend, err = backend.CreateVolumeFile(v.fileName+".dat", preallocate, 0); err != nil {
			return nil, fmt.Errorf("cannot create cache file %s.dat: %v", v.fileName, err)
		}
		v.lastModTime = time.Now()
	}

	var indexFile *os.File
	if indexFile, err = os.OpenFile(v.fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot write cache index %s.idx: %v", v.fileName, err)
	}

	glog.V(0).Infoln("loading leveldb", v.fileName+".ldb")
	opts := &opt.Options{
		BlockCacheCapacity:            2 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:                   1 * 1024 * 1024, // default value is 4MiB
		CompactionTableSizeMultiplier: 10,              // default value is 1
	}
	/// 将 .idx 文件中的数据 更新到 leveldb 中去
	if v.nm, err = storage.NewLevelDbNeedleMap(v.fileName+".ldb", indexFile, opts); err != nil {
		return nil, fmt.Errorf("loading leveldb %s error: %v", v.fileName+".ldb", err)
	}

	return v, nil

}

func (v *ChunkCacheVolume) Shutdown() {
	if v.DataBackend != nil {
		v.DataBackend.Close()
		v.DataBackend = nil
	}
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
}

func (v *ChunkCacheVolume) destroy() {
	v.Shutdown()
	os.Remove(v.fileName + ".dat")
	os.Remove(v.fileName + ".idx")
	os.RemoveAll(v.fileName + ".ldb")
}

func (v *ChunkCacheVolume) Reset() (*ChunkCacheVolume, error) {
	v.destroy()
	return LoadOrCreateChunkCacheVolume(v.fileName, v.sizeLimit)
}

/// 获取一个 needle 文件数据, 首先从 leveldb(.idx文件同步过去的) 中获取 在 .dat 文件中的偏移量和数据大小, 然后去 .dat 文件中读取
func (v *ChunkCacheVolume) GetNeedle(key types.NeedleId) ([]byte, error) {

	nv, ok := v.nm.Get(key)
	if !ok {
		return nil, storage.ErrorNotFound
	}
	data := make([]byte, nv.Size)
	if readSize, readErr := v.DataBackend.ReadAt(data, nv.Offset.ToAcutalOffset()); readErr != nil {
		return nil, fmt.Errorf("read %s.dat [%d,%d): %v",
			v.fileName, nv.Offset.ToAcutalOffset(), nv.Offset.ToAcutalOffset()+int64(nv.Size), readErr)
	} else {
		if readSize != int(nv.Size) {
			return nil, fmt.Errorf("read %d, expected %d", readSize, nv.Size)
		}
	}

	return data, nil
}

/// 写入一个 needle 文件数据, 往 .dat 文件中写入数据 和 必要的 填充数据, 然后将索引数据写入 leveldb 中去
func (v *ChunkCacheVolume) WriteNeedle(key types.NeedleId, data []byte) error {

	offset := v.fileSize

	written, err := v.DataBackend.WriteAt(data, offset)
	if err != nil {
		return err
	} else if written != len(data) {
		return fmt.Errorf("partial written %d, expected %d", written, len(data))
	}

	v.fileSize += int64(written)
	extraSize := written % types.NeedlePaddingSize
	if extraSize != 0 {
		v.DataBackend.WriteAt(v.smallBuffer[:types.NeedlePaddingSize-extraSize], offset+int64(written))
		v.fileSize += int64(types.NeedlePaddingSize - extraSize)
	}

	if err := v.nm.Put(key, types.ToOffset(offset), uint32(len(data))); err != nil {
		glog.V(4).Infof("failed to save in needle map %d: %v", key, err)
	}

	return nil
}
