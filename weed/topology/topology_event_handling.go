package topology

import (
	"google.golang.org/grpc"
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

/// 收集 dead node 和 满 volume, 进行 空洞 压缩, 只有 leader 才执行
func (t *Topology) StartRefreshWritableVolumes(grpcDialOption grpc.DialOption, garbageThreshold float64, preallocate int64) {
	go func() {
		for {
			if t.IsLeader() {
				freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
				/// 会遍历本地内存中的所有 data node, 如果 该节点 已满, 则将节点 写入 chanFullVolumes, 会在 下面 SetVolumeCapacityFull 中处理
				/// 将 超过 大小的 storage.VolumeInfo 写入 chanFullVolumes
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func(garbageThreshold float64) {
		c := time.Tick(15 * time.Minute)
		for _ = range c {
			if t.IsLeader() {
				/// 会调用 各个 volume server 进行 空洞 的 整理, 采用 复制算法, 新建临时文件, 将还在用的 volume 腾挪到临时文件
				t.Vacuum(grpcDialOption, garbageThreshold, preallocate)
			}
		}
	}(garbageThreshold)
	go func() {
		for {
			select {
			/// channel 中 保存 的 是 storage.VolumeInfo
			case v := <-t.chanFullVolumes:
				/// 将 volume 从 volume layout 的可写文件列表中删除
				t.SetVolumeCapacityFull(v)
			}
		}
	}()
}

/// 实际是将 一个 volume 从 Layout 的 writable 列表中删除
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, dn := range vl.vid2location[volumeInfo.Id].list {
		if !volumeInfo.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	return true
}

/// 从 VolumeLocationList 中 remove, 再从 VolumeLayout 的 可写列表中删除
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustRemoteVolumeCountDelta(-dn.GetRemoteVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}
