package util

import (
	"path/filepath"
	"strings"
)

type FullPath string

/// 完整路径相关
func NewFullPath(dir, name string) FullPath {
	return FullPath(dir).Child(name)
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

func (fp FullPath) Name() string {
	_, name := filepath.Split(string(fp))
	return name
}

func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)
	if strings.HasSuffix(dir, "/") {
		return FullPath(dir + name)
	}
	return FullPath(dir + "/" + name)
}

/// 获取 一个 路径的 uint64 型 摘要
func (fp FullPath) AsInode() uint64 {
	return uint64(HashStringToLong(string(fp)))
}

// split, but skipping the root
func (fp FullPath) Split() []string {
	if fp == "" || fp == "/" {
		return []string{}
	}
	return strings.Split(string(fp)[1:], "/")
}

func Join(names ...string) string {
	return filepath.ToSlash(filepath.Join(names...))
}

func JoinPath(names ...string) FullPath {
	return FullPath(Join(names...))
}
