package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 同一个 volume id 对应 多个 节点
type VolumeLocationList struct {
	list []*DataNode
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (dnll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", dnll.list)
}

func (dnll *VolumeLocationList) Head() *DataNode {
	//mark first node as master volume
	return dnll.list[0]
}

func (dnll *VolumeLocationList) Length() int {
	return len(dnll.list)
}

/// 将一个节点 data node 加入 一个 volume
func (dnll *VolumeLocationList) Set(loc *DataNode) {
	for i := 0; i < len(dnll.list); i++ {
		if loc.Ip == dnll.list[i].Ip && loc.Port == dnll.list[i].Port {
			dnll.list[i] = loc
			return
		}
	}
	dnll.list = append(dnll.list, loc)
}

func (dnll *VolumeLocationList) Remove(loc *DataNode) bool {
	for i, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			return true
		}
	}
	return false
}

func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			changed = true
			break
		}
	}
	if changed {
		var l []*DataNode
		for _, dnl := range dnll.list {
			if dnl.LastSeen >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		dnll.list = l
	}
}

/// 获取统计信息
func (dnll *VolumeLocationList) Stats(vid needle.VolumeId, freshThreshHold int64) (size uint64, fileCount int) {
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			/// 调用 DataNode 的 GetVolumesById
			vinfo, err := dnl.GetVolumesById(vid)
			if err == nil {
				return vinfo.Size - vinfo.DeletedByteCount, vinfo.FileCount - vinfo.DeleteCount
			}
		}
	}
	return 0, 0
}
