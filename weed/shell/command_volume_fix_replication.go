package shell

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

func init() {
	Commands = append(Commands, &commandVolumeFixReplication{})
}

type commandVolumeFixReplication struct {
}

func (c *commandVolumeFixReplication) Name() string {
	return "volume.fix.replication"
}

func (c *commandVolumeFixReplication) Help() string {
	return `add replicas to volumes that are missing replicas

	This command file all under-replicated volumes, and find volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	volume.fix.replication -n # do not take action
	volume.fix.replication    # actually copying the volume files and mount the volume

	Note:
		* each time this will only add back one replica for one volume id. If there are multiple replicas
		  are missing, e.g. multiple volume servers are new, you may need to run this multiple times.
		* do not run this too quick within seconds, since the new volume replica may take a few seconds 
		  to register itself to the master.

`
}

/// 找到 所有 副本 不足的 volume, 并且 找到 合适 的 data center \ rack \ node 进行拷贝以增加副本, FreeVolumeCount 越多的 node 优先级越高
func (c *commandVolumeFixReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	takeAction := true
	if len(args) > 0 && args[0] == "-n" {
		takeAction = false
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	// find all volumes that needs replication
	// collect all data nodes
	replicatedVolumeLocations := make(map[uint32][]location)
	replicatedVolumeInfo := make(map[uint32]*master_pb.VolumeInformationMessage)
	var allLocations []location
	/// 找到 所有 需要 备份 的 volume 及其 所在的 location {data center \ rack \ data node}
	eachDataNode(resp.TopologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(dc, string(rack), dn)
		for _, v := range dn.VolumeInfos {
			if v.ReplicaPlacement > 0 {
				/// 每个 volume id 有哪些 location
				replicatedVolumeLocations[v.Id] = append(replicatedVolumeLocations[v.Id], loc)
				replicatedVolumeInfo[v.Id] = v
			}
		}
		allLocations = append(allLocations, loc)
	})

	// find all under replicated volumes
	underReplicatedVolumeLocations := make(map[uint32][]location)
	/// 获取 每个 volume 的 id 和 对应的 locations
	for vid, locations := range replicatedVolumeLocations {
		volumeInfo := replicatedVolumeInfo[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(volumeInfo.ReplicaPlacement))
		/// 记录哪些 volume 的 应有备份数 大于 已有备份数, 即需要增加备份的 volume
		if replicaPlacement.GetCopyCount() > len(locations) {
			underReplicatedVolumeLocations[vid] = locations
		}
	}

	if len(underReplicatedVolumeLocations) == 0 {
		return fmt.Errorf("no under replicated volumes")
	}

	if len(allLocations) == 0 {
		return fmt.Errorf("no data nodes at all")
	}

	// find the most under populated data nodes
	/// 按 各个 node 的 FreeVolumeCount 降序排序, 也就说 越多 空闲的 优先级越高, 简单负载均衡
	keepDataNodesSorted(allLocations)

	/// 遍历需要增加备份的 volume
	for vid, locations := range underReplicatedVolumeLocations {
		volumeInfo := replicatedVolumeInfo[vid]
		/// 应有备份数
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(volumeInfo.ReplicaPlacement))
		foundNewLocation := false
		/// 在所有 location 中进行查找, allLocations 按 各个 node 的 FreeVolumeCount 降序排序
		for _, dst := range allLocations {
			// check whether data nodes satisfy the constraints
			/// 通过 检查 已有 location (existingLocations) 中的 data center \ rack \ data node 数目, 检查 possibleLocation 是否满足特定的备份要求
			if dst.dataNode.FreeVolumeCount > 0 && satisfyReplicaPlacement(replicaPlacement, locations, dst) {
				// ask the volume server to replicate the volume
				/// 在 已有 的 node 中随机选择一个节点 作为 拷贝的 源 节点
				sourceNodes := underReplicatedVolumeLocations[vid]
				sourceNode := sourceNodes[rand.Intn(len(sourceNodes))]
				foundNewLocation = true
				fmt.Fprintf(writer, "replicating volume %d %s from %s to dataNode %s ...\n", volumeInfo.Id, replicaPlacement, sourceNode.dataNode.Id, dst.dataNode.Id)

				if !takeAction {
					break
				}

				/// 向目标节点 发送 请求, 让目标 节点 去拷贝 源节点
				err := operation.WithVolumeServerClient(dst.dataNode.Id, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
					_, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
						VolumeId:       volumeInfo.Id,
						SourceDataNode: sourceNode.dataNode.Id,
					})
					return replicateErr
				})

				if err != nil {
					return err
				}

				// adjust free volume count
				dst.dataNode.FreeVolumeCount--
				/// 再次将 allLocations 按 各个 node 的 FreeVolumeCount 降序排序
				keepDataNodesSorted(allLocations)
				break
			}
		}
		if !foundNewLocation {
			fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", volumeInfo.Id, replicaPlacement, locations)
		}

	}

	return nil
}

func keepDataNodesSorted(dataNodes []location) {
	sort.Slice(dataNodes, func(i, j int) bool {
		return dataNodes[i].dataNode.FreeVolumeCount > dataNodes[j].dataNode.FreeVolumeCount
	})
}

/*
  if on an existing data node {
    return false
  }
  if different from existing dcs {
    if lack on different dcs {
      return true
    }else{
      return false
    }
  }
  if not on primary dc {
    return false
  }
  if different from existing racks {
    if lack on different racks {
      return true
    }else{
      return false
    }
  }
  if not on primary rack {
    return false
  }
  if lacks on same rack {
    return true
  } else {
    return false
  }
*/

/// 通过 检查 已有 location (existingLocations) 中的 data center \ rack \ data node 数目, 检查 possibleLocation 是否满足特定的备份要求
func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, existingLocations []location, possibleLocation location) bool {

	/// 统计 每个已存在 location 的 node 数目
	existingDataNodes := make(map[string]int)
	for _, loc := range existingLocations {
		existingDataNodes[loc.String()] += 1
	}
	/// 查看目标 location 是否已经在 数据中心 超过备份数目
	sameDataNodeCount := existingDataNodes[possibleLocation.String()]
	// avoid duplicated volume on the same data node
	if sameDataNodeCount > 0 {
		return false
	}

	/// 统计 哪些 data center 已存在多少个 节点
	existingDataCenters := make(map[string]int)
	for _, loc := range existingLocations {
		existingDataCenters[loc.DataCenter()] += 1
	}
	/// 统计有最大数目 node 的 data centers 的列表(可能多个 data center 有相同数目的 node)
	primaryDataCenters, _ := findTopKeys(existingDataCenters)

	// ensure data center count is within limit
	/// 目标数据中心 不在 已存在的数据中心 列表中, 可以扩展数据中心
	if _, found := existingDataCenters[possibleLocation.DataCenter()]; !found {
		// different from existing dcs
		/// 数据中心 层面 来说 还没有达到足够数目的 备份数
		if len(existingDataCenters) < replicaPlacement.DiffDataCenterCount+1 {
			// lack on different dcs
			return true
		} else {
			// adding this would go over the different dcs limit
			return false
		}
	}
	// now this is same as one of the existing data center
	/// 没在 primaryDataCenters 当中
	if !isAmong(possibleLocation.DataCenter(), primaryDataCenters) {
		// not on one of the primary dcs
		return false
	}

	// now this is one of the primary dcs
	/// 现在又开始检查机架
	existingRacks := make(map[string]int)
	for _, loc := range existingLocations {
		/// 定位目标 数据中心
		if loc.DataCenter() != possibleLocation.DataCenter() {
			continue
		}
		/// 统计 已经在 哪些 机架 中有 多少个节点
		existingRacks[loc.Rack()] += 1
	}
	/// 找到 含节点数 最多 的 机架
	primaryRacks, _ := findTopKeys(existingRacks)
	/// 目标机架已有的 node 数目
	sameRackCount := existingRacks[possibleLocation.Rack()]

	// ensure rack count is within limit
	if _, found := existingRacks[possibleLocation.Rack()]; !found {
		// different from existing racks
		/// 查看机架是否含有超过了配置数的节点
		if len(existingRacks) < replicaPlacement.DiffRackCount+1 {
			// lack on different racks
			return true
		} else {
			// adding this would go over the different racks limit
			return false
		}
	}
	// now this is same as one of the existing racks
	if !isAmong(possibleLocation.Rack(), primaryRacks) {
		// not on the primary rack
		return false
	}

	// now this is on the primary rack

	// different from existing data nodes
	/// 再检查同一个机架是否含有 指定数 的 备份节点
	if sameRackCount < replicaPlacement.SameRackCount+1 {
		// lack on same rack
		return true
	} else {
		// adding this would go over the same data node limit
		return false
	}

}

func findTopKeys(m map[string]int) (topKeys []string, max int) {
	for k, c := range m {
		if max < c {
			topKeys = topKeys[:0]
			topKeys = append(topKeys, k)
			max = c
		} else if max == c {
			topKeys = append(topKeys, k)
		}
	}
	return
}

func isAmong(key string, keys []string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

type location struct {
	dc       string
	rack     string
	dataNode *master_pb.DataNodeInfo
}

func newLocation(dc, rack string, dataNode *master_pb.DataNodeInfo) location {
	return location{
		dc:       dc,
		rack:     rack,
		dataNode: dataNode,
	}
}

func (l location) String() string {
	return fmt.Sprintf("%s %s %s", l.dc, l.rack, l.dataNode.Id)
}

func (l location) Rack() string {
	return fmt.Sprintf("%s %s", l.dc, l.rack)
}

func (l location) DataCenter() string {
	return l.dc
}
