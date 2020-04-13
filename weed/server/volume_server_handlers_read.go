package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

/// 从 DiskLocation 中查找是否 有 volume, 然后从缓存中查找位置, 没有的话发送请求进行 跳转
/// 如果是本地的 则进行 cookie 和过去时间处理,
/// 之后 处理 分块 定点 传输 和 图片 大小 更改, 如果 需要 会根据 分块 id 从 master 处获取 资源位置 然后 到相应服务器 分块 读取 资源
func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {

	stats.VolumeServerRequestCounter.WithLabelValues("get").Inc()
	start := time.Now()
	defer func() { stats.VolumeServerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	n := new(needle.Needle)
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infoln("parsing error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infoln("parsing fid error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// glog.V(4).Infoln("volume", volumeId, "reading", n)
	/// 所有 DiskLocation 中查找是否 有 volume
	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)
	/// 如果没找到
	if !hasVolume && !hasEcVolume {
		if !vs.ReadRedirect {
			glog.V(2).Infoln("volume is not local:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		/// 查找 首先通过缓存 缓存中没有的话就发送请求
		lookupResult, err := operation.Lookup(vs.GetMaster(), volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].PublicUrl))
			u.Path = fmt.Sprintf("%s/%s,%s", u.Path, vid, fid)
			arg := url.Values{}
			if c := r.FormValue("collection"); c != "" {
				arg.Set("collection", c)
			}
			u.RawQuery = arg.Encode()
			/// 进行 跳转 到指定位置
			http.Redirect(w, r, u.String(), http.StatusMovedPermanently)

		} else {
			glog.V(2).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	cookie := n.Cookie
	var count int
	if hasVolume {
		/// 从 volume 中读取 needle
		count, err = vs.store.ReadVolumeNeedle(volumeId, n)
	} else if hasEcVolume {
		count, err = vs.store.ReadEcShardNeedle(volumeId, n)
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	if err != nil || count < 0 {
		glog.V(0).Infof("read %s isNormalVolume %v error: %v", r.URL.Path, hasVolume, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	/// 对比 cookie
	if n.Cookie != cookie {
		glog.V(0).Infof("request %s with cookie:%x expected:%x from %s agent %s", r.URL.Path, cookie, n.Cookie, r.RemoteAddr, r.UserAgent())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	/// 查看修改时间
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm == "\""+n.Etag()+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	/// 设置响应中 http 头部 的 ETag
	setEtag(w, n.Etag())

	/// 设置响应中 http 头部 的 字段
	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			w.Header().Set(k, v)
		}
	}

	/// 处理 分块 定点 传输 和 图片 大小 更改, 如果 需要 会根据 分块 id 从 master 处获取 资源位置 然后 分块 读取 资源
	if vs.tryHandleChunkedFile(n, filename, ext, w, r) {
		return
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = filepath.Ext(filename)
		}
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	if ext != ".gz" {
		if n.IsGzipped() {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				if _, _, _, shouldResize := shouldResizeImages(ext, r); shouldResize {
					if n.Data, err = util.UnGzipData(n.Data); err != nil {
						glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
					}
				} else {
					w.Header().Set("Content-Encoding", "gzip")
				}
			} else {
				if n.Data, err = util.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
				}
			}
		}
	}

	rs := conditionallyResizeImages(bytes.NewReader(n.Data), ext, r)

	if e := writeResponseContent(filename, mtype, rs, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
}

/// 处理 分块 定点 传输 和 图片 大小 更改, 如果 需要 会根据 分块 id 从 master 处获取 资源位置 然后 分块 读取 资源
func (vs *VolumeServer) tryHandleChunkedFile(n *needle.Needle, fileName string, ext string, w http.ResponseWriter, r *http.Request) (processed bool) {
	/// 不是 chunk file
	if !n.IsChunkedManifest() || r.URL.Query().Get("cm") == "false" {
		return false
	}

	/// 获取文件分块信息
	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsGzipped())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", r.URL.Path, e)
		return false
	}
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}

	if ext == "" {
		ext = filepath.Ext(fileName)
	}

	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	w.Header().Set("X-File-Store", "chunked")

	/// 获取 ChunkedFileReader
	chunkedFileReader := operation.NewChunkedFileReader(chunkManifest.Chunks, vs.GetMaster())
	defer chunkedFileReader.Close()

	/// 查看是否需要 改变 图片 大小 如果要 就从 ChunkedFileReader 中 master 处读取原始数据
	rs := conditionallyResizeImages(chunkedFileReader, ext, r)

	/// 写数据 处理分段 定点 传输
	if e := writeResponseContent(fileName, mType, rs, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

func conditionallyResizeImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker

	/// 查看是否需要 改变 图片 大小
	width, height, mode, shouldResize := shouldResizeImages(ext, r)
	if shouldResize {
		rs, _, _ = images.Resized(ext, originalDataReaderSeeker, width, height, mode)
	}
	return rs
}

/// 查看是否需要 改变 图片 大小
func shouldResizeImages(ext string, r *http.Request) (width, height int, mode string, shouldResize bool) {
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" {
		if r.FormValue("width") != "" {
			width, _ = strconv.Atoi(r.FormValue("width"))
		}
		if r.FormValue("height") != "" {
			height, _ = strconv.Atoi(r.FormValue("height"))
		}
	}
	mode = r.FormValue("mode")
	shouldResize = width > 0 || height > 0
	return
}

/// 响应请求
func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}

	adjustHeadersAfterHEAD(w, r, filename)

	/// 写数据 处理分段 定点传输
	processRangeRequest(r, w, totalSize, mimeType, func(writer io.Writer, offset int64, size int64) error {
		if _, e = rs.Seek(offset, 0); e != nil {
			return e
		}
		_, e = io.CopyN(writer, rs, size)
		return e
	})
	return nil
}
