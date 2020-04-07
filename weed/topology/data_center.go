package topology

import "github.com/chrislusf/seaweedfs/weed/pb/master_pb"

/// data center -> rack -> data node
type DataCenter struct {
	NodeImpl
}

func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)
	dc.nodeType = "DataCenter"
	dc.children = make(map[NodeId]Node)
	dc.NodeImpl.value = dc
	return dc
}

func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		if string(rack.Id()) == rackName {
			return rack
		}
	}
	rack := NewRack(rackName)
	/// 将 rack 加入 data center
	dc.LinkChildNode(rack)
	return rack
}

/// 获取数据中心的问题
func (dc *DataCenter) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = dc.Id()
	m["Max"] = dc.GetMaxVolumeCount()
	m["Free"] = dc.FreeSpace()
	var racks []interface{}
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		racks = append(racks, rack.ToMap())
	}
	m["Racks"] = racks
	return m
}

/// 获取数据中心的信息, 会调用 rack 的 ToRackInfo
func (dc *DataCenter) ToDataCenterInfo() *master_pb.DataCenterInfo {
	m := &master_pb.DataCenterInfo{
		Id:                string(dc.Id()),
		VolumeCount:       uint64(dc.GetVolumeCount()),
		MaxVolumeCount:    uint64(dc.GetMaxVolumeCount()),
		FreeVolumeCount:   uint64(dc.FreeSpace()),
		ActiveVolumeCount: uint64(dc.GetActiveVolumeCount()),
		RemoteVolumeCount: uint64(dc.GetRemoteVolumeCount()),
	}
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		m.RackInfos = append(m.RackInfos, rack.ToRackInfo())
	}
	return m
}
