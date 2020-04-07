package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	replicationAsMin         bool
	storageType2VolumeLayout *util.ConcurrentReadMap
}

func NewCollection(name string, volumeSizeLimit uint64, replicationAsMin bool) *Collection {
	c := &Collection{
		Name:             name,
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
	c.storageType2VolumeLayout = util.NewConcurrentReadMap()
	return c
}

func (c *Collection) String() string {
	return fmt.Sprintf("Name:%s, volumeSizeLimit:%d, storageType2VolumeLayout:%v", c.Name, c.volumeSizeLimit, c.storageType2VolumeLayout)
}

func (c *Collection) GetOrCreateVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL) *VolumeLayout {
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	vl := c.storageType2VolumeLayout.Get(keyString, func() interface{} {
		return NewVolumeLayout(rp, ttl, c.volumeSizeLimit, c.replicationAsMin)
	})
	return vl.(*VolumeLayout)
}

/// 被 topology 的 Lookup 调用
func (c *Collection) Lookup(vid needle.VolumeId) []*DataNode {
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			if list := vl.(*VolumeLayout).Lookup(vid); list != nil {
				return list
			}
		}
	}
	return nil
}

/// 列出所有 数据 节点, 调用 VolumeLayout 的 ListVolumeServers 在 map 中获取
func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			if list := vl.(*VolumeLayout).ListVolumeServers(); list != nil {
				nodes = append(nodes, list...)
			}
		}
	}
	return
}
