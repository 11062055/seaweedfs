// +build windows

package util

import (
	"os"
)

/// 获取 文件 的 uid 和 gid
func GetFileUidGid(fi os.FileInfo) (uid, gid uint32) {
	return 0, 0
}
