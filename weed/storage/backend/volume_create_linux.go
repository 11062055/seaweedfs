// +build linux

package backend

import (
	"os"
	"syscall"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

/// 创建 volume 文件
func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if e != nil {
		return nil, e
	}
	if preallocate != 0 {
		///  文件 预留 打洞
		syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)
		glog.V(0).Infof("Preallocated %d bytes disk space for %s", preallocate, fileName)
	}
	return NewDiskFile(file), nil
}
