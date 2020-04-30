package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/// Topology 也是 一个 虚拟节点 NodeImpl
type Topology struct {
	vacuumLockCounter int64
	NodeImpl

	collectionMap  *util.ConcurrentReadMap
	ecShardMap     map[needle.VolumeId]*EcShardLocations
	ecShardMapLock sync.RWMutex

	pulse int64

	volumeSizeLimit  uint64
	replicationAsMin bool

	Sequence sequence.Sequencer

	chanFullVolumes chan storage.VolumeInfo

	Configuration *Configuration

	RaftServer raft.Server
}

/// 比如 NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, ms.option.PulseSeconds, replicationAsMin)
/// topology 也是一个 node, node 的 类型 为 Topology, node 的 value 是 topology,
func NewTopology(id string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int, replicationAsMin bool) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentReadMap()
	t.ecShardMap = make(map[needle.VolumeId]*EcShardLocations)
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.replicationAsMin = replicationAsMin

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)

	t.Configuration = &Configuration{}

	return t
}

/// 从 raft 中查看当前节点是不是 leader 节点
func (t *Topology) IsLeader() bool {
	if t.RaftServer != nil {
		if t.RaftServer.State() == raft.Leader {
			return true
		}
		if t.RaftServer.Leader() == "" {
			return true
		}
	}
	return false
}

/// 通过 raft 获取 leader
func (t *Topology) Leader() (string, error) {
	l := ""
	if t.RaftServer != nil {
		l = t.RaftServer.Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}

	if l == "" {
		// We are a single node cluster, we are the leader
		return t.RaftServer.Name(), nil
	}

	return l, nil
}

/// 查找 特定 volume id 的位置, 调用 Collection 的 Lookup 和 VolumeLayout 的 Lookup 方法
func (t *Topology) Lookup(collection string, vid needle.VolumeId) (dataNodes []*DataNode) {
	//maybe an issue if lots of collections?
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}

	if locations, found := t.LookupEcShards(vid); found {
		for _, loc := range locations.Locations {
			dataNodes = append(dataNodes, loc...)
		}
		return dataNodes
	}

	return nil
}

func (t *Topology) NextVolumeId() (needle.VolumeId, error) {
	vid := t.GetMaxVolumeId()
	next := vid.Next()
	if _, err := t.RaftServer.Do(NewMaxVolumeIdCommand(next)); err != nil {
		return 0, err
	}
	return next, nil
}

/// 查看是否有可写的 volume
func (t *Topology) HasWritableVolume(option *VolumeGrowOption) bool {
	/// 获取 VolumeLayout 布局
	vl := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

/// 获取指定个数的可写节点, 递归调用 VolumeLayout 的 PickForWrite, 本地操作
func (t *Topology) PickForWrite(count uint64, option *VolumeGrowOption) (string, uint64, *DataNode, error) {
	vid, count, datanodes, err := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl).PickForWrite(count, option)
	if err != nil {
		return "", 0, nil, fmt.Errorf("failed to find writable volumes for collection:%s replication:%s ttl:%s error: %v", option.Collection, option.ReplicaPlacement.String(), option.Ttl.String(), err)
	}
	if datanodes.Length() == 0 {
		return "", 0, nil, fmt.Errorf("no writable volumes available for collection:%s replication:%s ttl:%s", option.Collection, option.ReplicaPlacement.String(), option.Ttl.String())
	}
	fileId := t.Sequence.NextFileId(count)
	return needle.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
}

/// 获取 集合 collection 中的 VolumeLayout 没有则创建
func (t *Topology) GetVolumeLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl)
}

/// 从map中获取collection
func (t *Topology) ListCollections(includeNormalVolumes, includeEcVolumes bool) (ret []string) {

	mapOfCollections := make(map[string]bool)
	for _, c := range t.collectionMap.Items() {
		mapOfCollections[c.(*Collection).Name] = true
	}

	if includeEcVolumes {
		t.ecShardMapLock.RLock()
		for _, ecVolumeLocation := range t.ecShardMap {
			mapOfCollections[ecVolumeLocation.Collection] = true
		}
		t.ecShardMapLock.RUnlock()
	}

	for k := range mapOfCollections {
		ret = append(ret, k)
	}
	return ret
}

func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	if !hasCollection {
		return nil, false
	}
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	/// 将 data node 注册到 volume layout 中去
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).RegisterVolume(&v, dn)
}
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info:%+v", v)
	volumeLayout := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl)
	volumeLayout.UnRegisterVolume(&v, dn)
	if volumeLayout.isEmpty() {
		t.DeleteCollection(v.Collection)
	}
}

/// 创建 data center
func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	dc := NewDataCenter(dcName)
	/// 将 data center 加入 topology 中
	t.LinkChildNode(dc)
	return dc
}

/// 更新节点 和 该节点的 volume 信息
func (t *Topology) SyncDataNodeRegistration(volumes []*master_pb.VolumeInformationMessage, dn *DataNode) (newVolumes, deletedVolumes []storage.VolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var volumeInfos []storage.VolumeInfo
	for _, v := range volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// find out the delta volumes
	/// 更新节点的 volume 信息, 返回哪些是 新增的 哪些是删除 了的
	newVolumes, deletedVolumes = dn.UpdateVolumes(volumeInfos)
	/// 注册新增的节点
	for _, v := range newVolumes {
		t.RegisterVolumeLayout(v, dn)
	}
	/// 反注册删除了的节点
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}
	return
}

/// 更新 和 删除 volume 信息, 新增哪些 volume 和 删除了哪些 volume, 将信息更新到 volume layout 中去
func (t *Topology) IncrementalSyncDataNodeRegistration(newVolumes, deletedVolumes []*master_pb.VolumeShortInformationMessage, dn *DataNode) {
	var newVis, oldVis []storage.VolumeInfo
	for _, v := range newVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		newVis = append(newVis, vi)
	}
	for _, v := range deletedVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		oldVis = append(oldVis, vi)
	}
	/// 更新 或 删除 节点上的 volume 信息
	dn.DeltaUpdateVolumes(newVis, oldVis)

	for _, vi := range newVis {
		/// 注册 volume 到 layout 中去
		t.RegisterVolumeLayout(vi, dn)
	}
	for _, vi := range oldVis {
		/// 反注册 volume
		t.UnRegisterVolumeLayout(vi, dn)
	}

	return
}
