package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL|EACH_COLLECTION|<collection_name>] [-force] [-dataCenter=<data_center_name>]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			balanceWritableVolumes()
			balanceReadOnlyVolumes()
		}
	}

	func balanceWritableVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for hasMovedOneVolume {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}
	func balanceReadOnlyVolumes(){
		//similar to balanceWritableVolumes
	}

`
}

func (c *commandVolumeBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or use \"ALL_COLLECTIONS\" across collections, \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan.")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	/// 获取 volume list, 只有 leader master 才执行, 调用 topology 的 ToTopologyInfo 方法
	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	/// 在特定数据中心 根据每个 data node 节点 的 volume 数量 组织节点列表
	/// typeToNodes[dn.MaxVolumeCount] = append(typeToNodes[dn.MaxVolumeCount], &Node{ info: dn, dc: dc.Id, rack: r.Id})
	typeToNodes := collectVolumeServersByType(resp.TopologyInfo, *dc)

	for maxVolumeCount, volumeServers := range typeToNodes {
		if len(volumeServers) < 2 {
			fmt.Printf("only 1 node is configured max %d volumes, skipping balancing\n", maxVolumeCount)
			continue
		}
		/// 针对 每个 collection
		if *collection == "EACH_COLLECTION" {
			/// 列出所有 的 collection
			collections, err := ListCollectionNames(commandEnv, true, false)
			if err != nil {
				return err
			}
			/// 遍历 所有 collection
			for _, c := range collections {
				/// 将 volume 数目很多的节点中的 可读可写 volume 移动到 volume 数目较少的 某些节点中去, 以达到所有 节点的 volume 数目基本一致
				/// 迁移的方式是先全量, 然后根据一个时间戳 进行增量 tail
				if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, c, *applyBalancing); err != nil {
					return err
				}
			}
		/// 所有 collection
		} else if *collection == "ALL_COLLECTIONS" {
			if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, "ALL_COLLECTIONS", *applyBalancing); err != nil {
				return err
			}
		/// 命令行中特定的 collection
		} else {
			if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, *collection, *applyBalancing); err != nil {
				return err
			}
		}

	}
	return nil
}

/// 将 volume 数目很多的节点中的 可读可写 volume 移动到 volume 数目较少的 某些节点中去, 以达到所有 节点的 volume 数目基本一致
/// 迁移的方式是先全量, 然后根据一个时间戳 进行增量 tail
func balanceVolumeServers(commandEnv *CommandEnv, nodes []*Node, volumeSizeLimit uint64, collection string, applyBalancing bool) error {

	// balance writable volumes
	/// 处理可写节点
	for _, n := range nodes {
		/// node.selectedVolumes[v.Id] = v 将满足条件的 volume 保存在 node 的 selectedVolumes 中
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			/// 区别 是 所有 的 collection 还是 特定 collection
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return !v.ReadOnly && v.Size < volumeSizeLimit
		})
	}
	/// 将 volume 数目很多的节点中的 某些 volume 移动到 volume 数目较少的 某些节点中去, 以达到所有 节点的 volume 数目基本一致
	/// 迁移的方式是先全量, 然后根据一个时间戳 进行增量 tail
	if err := balanceSelectedVolume(commandEnv, nodes, sortWritableVolumes, applyBalancing); err != nil {
		return err
	}

	// balance readable volumes
	/// 处理可读性节点
	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return v.ReadOnly || v.Size >= volumeSizeLimit
		})
	}
	if err := balanceSelectedVolume(commandEnv, nodes, sortReadOnlyVolumes, applyBalancing); err != nil {
		return err
	}

	return nil
}

func collectVolumeServersByType(t *master_pb.TopologyInfo, selectedDataCenter string) (typeToNodes map[uint64][]*Node) {
	typeToNodes = make(map[uint64][]*Node)
	/// 遍历 data center
	for _, dc := range t.DataCenterInfos {
		if selectedDataCenter != "" && dc.Id != selectedDataCenter {
			continue
		}
		/// 找到了 目标 data center 再遍历 rack 机架
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				/// 根据每个 data node 节点 的 volume 数量 组织节点列表
				typeToNodes[dn.MaxVolumeCount] = append(typeToNodes[dn.MaxVolumeCount], &Node{
					info: dn,
					dc:   dc.Id,
					rack: r.Id,
				})
			}
		}
	}
	return
}

type Node struct {
	info            *master_pb.DataNodeInfo
	selectedVolumes map[uint32]*master_pb.VolumeInformationMessage
	dc              string
	rack            string
}

/// 可写 volume 按 size 大小排序
func sortWritableVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Size < volumes[j].Size
	})
}

/// 只读 volume 按 id 大小排序
func sortReadOnlyVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Id < volumes[j].Id
	})
}

/// 将 volume 数目很多的节点中的 某些 volume 移动到 volume 数目较少的 某些节点中去, 以达到所有 节点的 volume 数目基本一致
func balanceSelectedVolume(commandEnv *CommandEnv, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), applyBalancing bool) error {
	/// 统计 总的 volume 数目
	selectedVolumeCount := 0
	for _, dn := range nodes {
		selectedVolumeCount += len(dn.selectedVolumes)
	}

	/// 总的 volume 数目 除以 node 数目 得到 每个 node 的最佳 volume 数目
	idealSelectedVolumes := ceilDivide(selectedVolumeCount, len(nodes))

	hasMove := true

	for hasMove {
		hasMove = false
		/// 按 volume 数目 升序 排序
		sort.Slice(nodes, func(i, j int) bool {
			// TODO sort by free volume slots???
			return len(nodes[i].selectedVolumes) < len(nodes[j].selectedVolumes)
		})
		/// volume 数目最少 最多 的两个节点
		emptyNode, fullNode := nodes[0], nodes[len(nodes)-1]
		/// volume 数量 负载不 均衡
		if len(fullNode.selectedVolumes) > idealSelectedVolumes && len(emptyNode.selectedVolumes)+1 <= idealSelectedVolumes {

			// sort the volumes to move
			/// 拿出 volume 数目 最多的 节点 中的 所有 volume
			var candidateVolumes []*master_pb.VolumeInformationMessage
			for _, v := range fullNode.selectedVolumes {
				candidateVolumes = append(candidateVolumes, v)
			}
			/// 将 volume 最多的节点中的所有 volumes 拿出来, 可写 volume 按 size 大小排序 , 只读 volume 按 id 大小排序
			sortCandidatesFn(candidateVolumes)

			/// 遍历拿出的所有 volume
			for _, v := range candidateVolumes {
				if v.ReplicaPlacement > 0 {
					/// 已经在不同机架
					if fullNode.dc != emptyNode.dc && fullNode.rack != emptyNode.rack {
						// TODO this logic is too simple, but should work most of the time
						// Need a correct algorithm to handle all different cases
						continue
					}
				}
				/// 把该 volume 移动到 volume 数目最少的节点中去
				if _, found := emptyNode.selectedVolumes[v.Id]; !found {
					if err := moveVolume(commandEnv, v, fullNode, emptyNode, applyBalancing); err == nil {
						/// 删除 当前节点
						delete(fullNode.selectedVolumes, v.Id)
						emptyNode.selectedVolumes[v.Id] = v
						/// 下次还需要再遍历
						hasMove = true
						break
					} else {
						return err
					}
				}
			}
		}
	}
	return nil
}

func moveVolume(commandEnv *CommandEnv, v *master_pb.VolumeInformationMessage, fullNode *Node, emptyNode *Node, applyBalancing bool) error {
	collectionPrefix := v.Collection + "_"
	if v.Collection == "" {
		collectionPrefix = ""
	}
	fmt.Fprintf(os.Stdout, "moving volume %s%d %s => %s\n", collectionPrefix, v.Id, fullNode.info.Id, emptyNode.info.Id)
	if applyBalancing {
		/// 将一个 volume 从 一个 server 移动到 另一个 server, 首先进行 已有数据拷贝 并且记录最后一个 needle 的时间戳, 然后根据该纳秒时间戳进行增量 tail
		return LiveMoveVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(v.Id), fullNode.info.Id, emptyNode.info.Id, 5*time.Second)
	}
	return nil
}

func (node *Node) selectVolumes(fn func(v *master_pb.VolumeInformationMessage) bool) {
	node.selectedVolumes = make(map[uint32]*master_pb.VolumeInformationMessage)
	for _, v := range node.info.VolumeInfos {
		if fn(v) {
			node.selectedVolumes[v.Id] = v
		}
	}
}
