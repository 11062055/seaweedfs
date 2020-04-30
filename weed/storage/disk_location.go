package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 一个目录下有很多 volume
type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[needle.VolumeId]*Volume
	volumesLock    sync.RWMutex

	// erasure coding
	ecVolumes     map[needle.VolumeId]*erasure_coding.EcVolume
	ecVolumesLock sync.RWMutex
}

func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	return location
}

/// 形如 collection_volume.idx
func (l *DiskLocation) volumeIdFromPath(dir os.FileInfo) (needle.VolumeId, string, error) {
	name := dir.Name()
	if !dir.IsDir() && strings.HasSuffix(name, ".idx") {
		base := name[:len(name)-len(".idx")]
		collection, volumeId, err := parseCollectionVolumeId(base)
		return volumeId, collection, err
	}

	return 0, "", fmt.Errorf("Path is not a volume: %s", name)
}

/// 形如 collection_volume
func parseCollectionVolumeId(base string) (collection string, vid needle.VolumeId, err error) {
	i := strings.LastIndex(base, "_")
	if i > 0 {
		collection, base = base[0:i], base[i+1:]
	}
	vol, err := needle.NewVolumeId(base)
	return collection, vol, err
}

/// load 获取生成 一个 volume,
/// 从 .vif 文件中 解析 得到 VolumeInfo
/// 从 .dat 文件中 加载 得到 各个 needle 数据信息
/// 从 .dat 文件中读取超级块 信息
/// 从 .idx 文件中 获取 索引信息, 并保存 在 leveldb 中
/// 将 .idx 中的文件 的相关 数据 遍历 到 metric 中去
/// 并获取 最大 的 file key, 统计 所有文件总的 大小, 删除 文件 总的 次数, 删除 文件 总的 大小
func (l *DiskLocation) loadExistingVolume(fileInfo os.FileInfo, needleMapKind NeedleMapType) bool {
	name := fileInfo.Name()
	if !fileInfo.IsDir() && strings.HasSuffix(name, ".idx") {
		vid, collection, err := l.volumeIdFromPath(fileInfo)
		if err != nil {
			glog.Warningf("get volume id failed, %s, err : %s", name, err)
			return false
		}

		// void loading one volume more than once
		l.volumesLock.RLock()
		_, found := l.volumes[vid]
		l.volumesLock.RUnlock()
		if found {
			glog.V(1).Infof("loaded volume, %v", vid)
			return true
		}

		/// 从 .vif 文件中 解析 得到 VolumeInfo
		/// 从 .dat 文件中 加载 得到 各个 needle 数据信息
		/// 从 .dat 文件中读取超级块 信息
		/// 从 .idx 文件中 获取 索引信息, 并保存 在 leveldb 中
		/// 将 .idx 中的文件 的相关 数据 遍历 到 metric 中去
		/// 并获取 最大 的 file key, 统计 所有文件总的 大小, 删除 文件 总的 次数, 删除 文件 总的 大小
		v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil, 0, 0)
		if e != nil {
			glog.V(0).Infof("new volume %s error %s", name, e)
			return false
		}

		l.volumesLock.Lock()
		l.volumes[vid] = v
		l.volumesLock.Unlock()
		size, _, _ := v.FileStat()
		glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s",
			l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), size, v.Ttl.String())
		return true
	}
	return false
}

/// 并行 读取 目录 和 子目录
func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapType, concurrency int) {

	task_queue := make(chan os.FileInfo, 10*concurrency)
	go func() {
		if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
			for _, dir := range dirs {
				task_queue <- dir
			}
		}
		close(task_queue)
	}()

	var wg sync.WaitGroup
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range task_queue {
				_ = l.loadExistingVolume(dir, needleMapKind)
			}
		}()
	}
	wg.Wait()

}

/// 并行读取目录 下的 所有 volume 信息
func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {

	l.concurrentLoadingVolumes(needleMapKind, 10)
	glog.V(0).Infof("Store started on dir: %s with %d volumes max %d", l.Directory, len(l.volumes), l.MaxVolumeCount)

	l.loadAllEcShards()
	glog.V(0).Infof("Store started on dir: %s with %d ec shards", l.Directory, len(l.ecVolumes))

}

/// 从 硬盘 中删除 collection
func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {

	l.volumesLock.Lock()
	/// 收集哪些 volume 需要 被 删除 除了正在compacting的
	delVolsMap := l.unmountVolumeByCollection(collection)
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	/// 收集哪些 ec volume 需要 被 删除 除了正在compacting的
	delEcVolsMap := l.unmountEcVolumeByCollection(collection)
	l.ecVolumesLock.Unlock()

	errChain := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for _, v := range delVolsMap {
			/// 调用 volume 的 Destroy 直接从硬盘中删除 volume 相关数据
			if err := v.Destroy(); err != nil {
				errChain <- err
			}
		}
		wg.Done()
	}()

	go func() {
		for _, v := range delEcVolsMap {
			/// 调用 EcVolume 的 Destroy(), 其中会递归调用 EcVolumeShard 的 Destroy 方法
			v.Destroy()
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(errChain)
	}()

	errBuilder := strings.Builder{}
	for err := range errChain {
		errBuilder.WriteString(err.Error())
		errBuilder.WriteString("; ")
	}
	if errBuilder.Len() > 0 {
		e = fmt.Errorf(errBuilder.String())
	}

	return
}

func (l *DiskLocation) deleteVolumeById(vid needle.VolumeId) (found bool, e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy()
	if e != nil {
		return
	}
	found = true
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) LoadVolume(vid needle.VolumeId, needleMapKind NeedleMapType) bool {
	/// 读取卷信息
	if fileInfo, found := l.LocateVolume(vid); found {
		/// 递归加载卷
		return l.loadExistingVolume(fileInfo, needleMapKind)
	}
	return false
}

func (l *DiskLocation) DeleteVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not found, VolumeId: %d", vid)
	}
	_, err := l.deleteVolumeById(vid)
	return err
}

func (l *DiskLocation) UnloadVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not loaded, VolumeId: %d", vid)
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

/// 收集哪些 volume 需要 被 解挂, 从内存 volume 列表中删除
func (l *DiskLocation) unmountVolumeByCollection(collectionName string) map[needle.VolumeId]*Volume {
	deltaVols := make(map[needle.VolumeId]*Volume, 0)
	for k, v := range l.volumes {
		if v.Collection == collectionName && !v.isCompacting {
			deltaVols[k] = v
		}
	}

	for k := range deltaVols {
		delete(l.volumes, k)
	}
	return deltaVols
}

func (l *DiskLocation) SetVolume(vid needle.VolumeId, volume *Volume) {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	l.volumes[vid] = volume
}

func (l *DiskLocation) FindVolume(vid needle.VolumeId) (*Volume, bool) {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

func (l *DiskLocation) VolumesLen() int {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	return len(l.volumes)
}

func (l *DiskLocation) Close() {
	l.volumesLock.Lock()
	for _, v := range l.volumes {
		v.Close()
	}
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	for _, ecVolume := range l.ecVolumes {
		ecVolume.Close()
	}
	l.ecVolumesLock.Unlock()

	return
}

/// 读取卷信息
func (l *DiskLocation) LocateVolume(vid needle.VolumeId) (os.FileInfo, bool) {
	if fileInfos, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, fileInfo := range fileInfos {
			volId, _, err := l.volumeIdFromPath(fileInfo)
			if vid == volId && err == nil {
				return fileInfo, true
			}
		}
	}

	return nil, false
}

/// 统计 各个 volume 的未使用空间大小的和, 每个 volume 未使用空间大小 为 volumeSizeLimit - (datSize + idxSize)
func (l *DiskLocation) UnUsedSpace(volumeSizeLimit uint64) (unUsedSpace uint64) {

	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	for _, vol := range l.volumes {
		if vol.IsReadOnly() {
			continue
		}
		/// 分别获取每个 volume 的 .dat .idx 文件的大小, 每个 volume 未使用的空间大小就是 volumeSizeLimit - (datSize + idxSize)
		datSize, idxSize, _ := vol.FileStat()
		unUsedSpace += volumeSizeLimit - (datSize + idxSize)
	}

	return
}
