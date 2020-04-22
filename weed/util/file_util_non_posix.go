// +build linux darwin freebsd netbsd openbsd plan9 solaris zos

package util

import (
	"os"
	"syscall"
)

/// 获取 文件的 uid 和 gid
func GetFileUidGid(fi os.FileInfo) (uid, gid uint32) {
	return fi.Sys().(*syscall.Stat_t).Uid, fi.Sys().(*syscall.Stat_t).Gid
}
