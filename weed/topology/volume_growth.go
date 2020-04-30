package topology

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowOption struct {
	Collection         string
	ReplicaPlacement   *super_block.ReplicaPlacement
	Ttl                *needle.TTL
	Prealloacte        int64
	DataCenter         string
	Rack               string
	DataNode           string
	MemoryMapMaxSizeMb uint32
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

func (o *VolumeGrowOption) String() string {
	return fmt.Sprintf("Collection:%s, ReplicaPlacement:%v, Ttl:%v, DataCenter:%s, Rack:%s, DataNode:%s", o.Collection, o.ReplicaPlacement, o.Ttl, o.DataCenter, o.Rack, o.DataNode)
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
/// 问题是 实际 volume 和 逻辑 volume 的 区别是什么
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count int) {
	v := util.GetViper()
	v.SetDefault("master.volume_growth.copy_1", 7)
	v.SetDefault("master.volume_growth.copy_2", 6)
	v.SetDefault("master.volume_growth.copy_3", 3)
	v.SetDefault("master.volume_growth.copy_other", 1)
	switch copyCount {
	case 1:
		count = v.GetInt("master.volume_growth.copy_1")
	case 2:
		count = v.GetInt("master.volume_growth.copy_2")
	case 3:
		count = v.GetInt("master.volume_growth.copy_3")
	default:
		count = v.GetInt("master.volume_growth.copy_other")
	}
	return
}

/// 扩容 实际是更改本地 相关 node 的 参数, 在本地通过 data center \ rack \ data node 层层找到符合要求的节点, 然后向目标节点发起 pb 请求申请扩容
/// 最终调用 volume server 的 AllocateVolume
func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, grpcDialOption grpc.DialOption, topo *Topology, targetCount int) (count int, err error) {
	if targetCount == 0 {
		/// 通过 实际 volume 数目 找到 逻辑 volume 的 数目
		targetCount = vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount())
	}
	count, err = vg.GrowByCountAndType(grpcDialOption, targetCount, option, topo)
	if count > 0 && count%option.ReplicaPlacement.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}
/// 扩容 在本地通过 data center \ rack \ data node 层层找到符合要求的节点, 然后向目标节点发起 pb 请求申请扩容
/// 最终调用 volume server 的 AllocateVolume
func (vg *VolumeGrowth) GrowByCountAndType(grpcDialOption grpc.DialOption, targetCount int, option *VolumeGrowOption, topo *Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if c, e := vg.findAndGrow(grpcDialOption, topo, option); e == nil {
			counter += c
		} else {
			glog.V(0).Infof("create %d volume, created %d: %v", targetCount, counter, e)
			return counter, e
		}
	}
	return
}

/// 在本地通过 data center \ rack \ data node 层层找到符合要求的节点, 然后向目标节点发起 pb 请求申请扩容
/// 最终调用 volume server 的 AllocateVolume
func (vg *VolumeGrowth) findAndGrow(grpcDialOption grpc.DialOption, topo *Topology, option *VolumeGrowOption) (int, error) {
	/// 这里会层层调用 data center \ rack \ data node 的相关函数以确定是否有合符规定的节点, 都是本地操作
	servers, e := vg.findEmptySlotsForOneVolume(topo, option)
	if e != nil {
		return 0, e
	}
	vid, raftErr := topo.NextVolumeId()
	if raftErr != nil {
		return 0, raftErr
	}
	/// 这里向目标节点发起 pb 请求申请扩容
	/// 最终调用 volume server 的 AllocateVolume
	err := vg.grow(grpcDialOption, topo, vid, option, servers...)
	return len(servers), err
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
/// 层层调用 data center \ rack \ data node 的 PickNodesByWeight 以确定是否有合符规定的节点, 都是本地操作
func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption) (servers []*DataNode, err error) {
	//find main datacenter and other data centers
	rp := option.ReplicaPlacement
	/// 获取包含 rp.DiffDataCenterCount+1 个节点 且满足 函数 func 的数据中心节点
	mainDataCenter, otherDataCenters, dc_err := topo.PickNodesByWeight(rp.DiffDataCenterCount+1, func(node Node) error {
		/// 数据中心不匹配
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}
		/// 机架 数目 不匹配
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}
		/// 剩余空间不匹配
		if node.FreeSpace() < int64(rp.DiffRackCount+rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.DiffRackCount+rp.SameRackCount+1)
		}
		/// 检查 rack 数目够不够
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if n.FreeSpace() >= 1 {
					possibleDataNodesCount++
				}
			}
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				/// 机架数加 1
				possibleRacksCount++
			}
		}
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		/// 机架数满足 说明找到了数据中心节点
		return nil
	})
	if dc_err != nil {
		return nil, dc_err
	}

	/// 相同的方式寻找机架节点
	//find main rack and other racks
	mainRack, otherRacks, rackErr := mainDataCenter.(*DataCenter).PickNodesByWeight(rp.DiffRackCount+1, func(node Node) error {
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}
		if node.FreeSpace() < int64(rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.SameRackCount+1)
		}
		if len(node.Children()) < rp.SameRackCount+1 {
			// a bit faster way to test free racks
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if n.FreeSpace() >= 1 {
				possibleDataNodesCount++
			}
		}
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rackErr != nil {
		return nil, rackErr
	}

	/// 相同的方式寻找 数据节点
	//find main rack and other racks
	mainServer, otherServers, serverErr := mainRack.(*Rack).PickNodesByWeight(rp.SameRackCount+1, func(node Node) error {
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}
		if node.FreeSpace() < 1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), 1)
		}
		return nil
	})
	if serverErr != nil {
		return nil, serverErr
	}

	/// 同一个 data center \ rack 中的 主节点, 和 其余满足条件的节点
	servers = append(servers, mainServer.(*DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}
	/// 同一个 data center 其余机架中每个机架一个节点
	for _, rack := range otherRacks {
		r := rand.Int63n(rack.FreeSpace())
		/// 会递归调用 每个rack节点的子data节点 检查总计是否有 r 大小的空闲空间
		if server, e := rack.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	/// 其它 data center 每个 data center 一个节点
	for _, datacenter := range otherDataCenters {
		r := rand.Int63n(datacenter.FreeSpace())
		/// 会递归调用 每个datacenter节点的子rank节点 检查总计是否有 r 大小的空闲空间, 然后获取 一个 data node
		if server, e := datacenter.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	return
}

/// 扩容 最终调用 volume server 的 AllocateVolume
func (vg *VolumeGrowth) grow(grpcDialOption grpc.DialOption, topo *Topology, vid needle.VolumeId, option *VolumeGrowOption, servers ...*DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, grpcDialOption, vid, option); err == nil {
			vi := storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          needle.CurrentVersion,
			}
			/// 记录 volume 的信息, 更新相关统计数据
			server.AddOrUpdateVolume(vi)
			/// 并且更新到 volume layout 中去
			/// 由于 AllocateVolume 实际是远程调用 volume server 的 AllocateVolume, volume server 会通过 SendHeartbeat 方式
			/// 将新增的 volume 再次上报给 leader master, 然后 leader master 还会再次注册到 volume layout 中去
			/// 并且 leader master 会 通过 KeepConnected 向其它 master 通知 新增加的 volume 信息, 这样 就实现了 数据的同步
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server.NodeImpl.String())
		} else {
			glog.V(0).Infoln("Failed to assign volume", vid, "to", servers, "error", err)
			return fmt.Errorf("Failed to assign %d: %v", vid, err)
		}
	}
	return nil
}
