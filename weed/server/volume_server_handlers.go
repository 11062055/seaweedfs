package weed_server

import (
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
)

/*

If volume server is started with a separated public port, the public port will
be more "secure".

Public port currently only supports reads.

Later writes on public port can have one of the 3
security settings:
1. not secured
2. secured by white list
3. secured by JWT(Json Web Token)

*/

/// 用于处理内部后端请求 包括 volume 及其 副本的 增 删 查, 图片的 缩放扩大等, 若非本地则返回 跳转
func (vs *VolumeServer) privateStoreHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET", "HEAD":
		stats.ReadRequest()
		/// 从 DiskLocation 中查找是否 有 volume, 然后从缓存中查找位置, 没有的话发送请求进行 跳转
		/// 如果是本地的 则进行 cookie 和过去时间处理,
		/// 之后 处理 分块 定点 传输 和 图片 大小 更改, 如果 需要 会根据 分块 id 从 master 处获取 资源位置 然后 到相应服务器 分块 读取 资源
		vs.GetOrHeadHandler(w, r)
	case "DELETE":
		stats.DeleteRequest()
		/// 删除 volume 包括 副本 本地数据 和远端数据
		vs.guard.WhiteList(vs.DeleteHandler)(w, r)
	case "PUT", "POST":
		stats.WriteRequest()
		/// 上传数据 包括写本地和远端副本
		vs.guard.WhiteList(vs.PostHandler)(w, r)
	}
}

func (vs *VolumeServer) publicReadOnlyHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		stats.ReadRequest()
		/// 从 DiskLocation 中查找是否 有 volume, 然后从缓存中查找位置, 没有的话发送请求进行 跳转
		/// 如果是本地的 则进行 cookie 和过去时间处理,
		/// 之后 处理 分块 定点 传输 和 图片 大小 更改, 如果 需要 会根据 分块 id 从 master 处获取 资源位置 然后 到相应服务器 分块 读取 资源
		vs.GetOrHeadHandler(w, r)
	case "HEAD":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	}
}

func (vs *VolumeServer) maybeCheckJwtAuthorization(r *http.Request, vid, fid string, isWrite bool) bool {

	var signingKey security.SigningKey

	if isWrite {
		if len(vs.guard.SigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.SigningKey
		}
	} else {
		if len(vs.guard.ReadSigningKey) == 0 {
			return true
		} else {
			signingKey = vs.guard.ReadSigningKey
		}
	}

	tokenStr := security.GetJwt(r)
	if tokenStr == "" {
		glog.V(1).Infof("missing jwt from %s", r.RemoteAddr)
		return false
	}

	token, err := security.DecodeJwt(signingKey, tokenStr)
	if err != nil {
		glog.V(1).Infof("jwt verification error from %s: %v", r.RemoteAddr, err)
		return false
	}
	if !token.Valid {
		glog.V(1).Infof("jwt invalid from %s: %v", r.RemoteAddr, tokenStr)
		return false
	}

	if sc, ok := token.Claims.(*security.SeaweedFileIdClaims); ok {
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			fid = fid[:sepIndex]
		}
		return sc.Fid == vid+","+fid
	}
	glog.V(1).Infof("unexpected jwt from %s: %v", r.RemoteAddr, tokenStr)
	return false
}
