package util

import (
	"errors"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

/// 检查目录是否可写
func TestFolderWritable(folder string) (err error) {
	fileInfo, err := os.Stat(folder)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return errors.New("Not a valid folder!")
	}
	perm := fileInfo.Mode().Perm()
	glog.V(0).Infoln("Folder", folder, "Permission:", perm)
	if 0200&perm != 0 {
		return nil
	}
	return errors.New("Not writable!")
}

/// 获取文件大小
func GetFileSize(file *os.File) (size int64, err error) {
	var fi os.FileInfo
	if fi, err = file.Stat(); err == nil {
		size = fi.Size()
	}
	return
}

/// 检查文件是否存在
func FileExists(filename string) bool {

	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true

}

/// 获取文件信息 返回 是否存在 可读 可写 修改时间 文件大小
func CheckFile(filename string) (exists, canRead, canWrite bool, modTime time.Time, fileSize int64) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	fileSize = fi.Size()
	return
}
