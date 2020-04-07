package topology

import "github.com/chrislusf/seaweedfs/weed/pb/master_pb"

/// uiStatusHandler中调用该方法获取拓扑信息
func (t *Topology) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.GetMaxVolumeCount()
	m["Free"] = t.FreeSpace()
	var dcs []interface{}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		/// 递归调用 数据中心 机架 节点 的 ToMap 方法获取所有信息
		dcs = append(dcs, dc.ToMap())
	}
	m["DataCenters"] = dcs
	var layouts []interface{}
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		for _, layout := range c.storageType2VolumeLayout.Items() {
			if layout != nil {
				tmp := layout.(*VolumeLayout).ToMap()
				tmp["collection"] = c.Name
				layouts = append(layouts, tmp)
			}
		}
	}
	m["Layouts"] = layouts
	return m
}

func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.GetMaxVolumeCount()
	m["Free"] = t.FreeSpace()
	dcs := make(map[NodeId]interface{})
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		racks := make(map[NodeId]interface{})
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			dataNodes := make(map[NodeId]interface{})
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				var volumes []interface{}
				for _, v := range dn.GetVolumes() {
					volumes = append(volumes, v)
				}
				dataNodes[d.Id()] = volumes
			}
			racks[r.Id()] = dataNodes
		}
		dcs[dc.Id()] = racks
	}
	m["DataCenters"] = dcs
	return m
}

/// 获取 volume 信息 和 ec shard 信息
func (t *Topology) ToVolumeLocations() (volumeLocations []*master_pb.VolumeLocation) {
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				volumeLocation := &master_pb.VolumeLocation{
					Url:       dn.Url(),
					PublicUrl: dn.PublicUrl,
				}
				for _, v := range dn.GetVolumes() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(v.Id))
				}
				for _, s := range dn.GetEcShards() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(s.VolumeId))
				}
				volumeLocations = append(volumeLocations, volumeLocation)
			}
		}
	}
	return
}

/// 获取 拓扑 信息, 会调用 data center 的 ToDataCenterInfo, 其中递归调用 rack 和 data node 的相关方法获取整个 拓扑的信息
func (t *Topology) ToTopologyInfo() *master_pb.TopologyInfo {
	m := &master_pb.TopologyInfo{
		Id:                string(t.Id()),
		VolumeCount:       uint64(t.GetVolumeCount()),
		MaxVolumeCount:    uint64(t.GetMaxVolumeCount()),
		FreeVolumeCount:   uint64(t.FreeSpace()),
		ActiveVolumeCount: uint64(t.GetActiveVolumeCount()),
		RemoteVolumeCount: uint64(t.GetRemoteVolumeCount()),
	}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		m.DataCenterInfos = append(m.DataCenterInfos, dc.ToDataCenterInfo())
	}
	return m
}
