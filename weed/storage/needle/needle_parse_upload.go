package needle

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type ParsedUpload struct {
	FileName         string
	Data             []byte
	MimeType         string
	PairMap          map[string]string
	IsGzipped        bool
	OriginalDataSize int
	ModifiedTime     uint64
	Ttl              *TTL
	IsChunkedFile    bool
	UncompressedData []byte
}

/// 解析上传 的 volume 数据
func ParseUpload(r *http.Request, sizeLimit int64) (pu *ParsedUpload, e error) {
	pu = &ParsedUpload{}
	pu.PairMap = make(map[string]string)
	/// 获取 http 请求 头部 的 Seaweed- 开头的所有字段
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pu.PairMap[k] = v[0]
		}
	}

	if r.Method == "POST" {
		/// 从 http 请求中 读取 数据
		/// 读取 固定 大小 的 数据, 其余的丢弃, 保存 在 pu.Data 中
		e = parseMultipart(r, sizeLimit, pu)
	} else {
		/// 读取 固定 大小 的 数据, 其余的丢弃, 保存 在 pu.Data 中
		e = parsePut(r, sizeLimit, pu)
	}
	if e != nil {
		return
	}

	pu.ModifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	pu.Ttl, _ = ReadTTL(r.FormValue("ttl"))

	/// 获取 数据, 区分压缩 和 没压缩 两种
	pu.OriginalDataSize = len(pu.Data)
	pu.UncompressedData = pu.Data
	if pu.IsGzipped {
		if unzipped, e := util.UnGzipData(pu.Data); e == nil {
			pu.OriginalDataSize = len(unzipped)
			pu.UncompressedData = unzipped
		}
	} else if shouldGzip, _ := util.IsGzippableFileType("", pu.MimeType); pu.MimeType == "" || shouldGzip {
		if compressedData, err := util.GzipData(pu.Data); err == nil {
			pu.Data = compressedData
			pu.IsGzipped = true
		}
	}

	return
}

/// 读取 固定 大小 的 数据, 其余的丢弃, 保存 在 pu.Data 中
func parsePut(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {
	pu.IsGzipped = r.Header.Get("Content-Encoding") == "gzip"
	pu.MimeType = r.Header.Get("Content-Type")
	pu.FileName = ""
	pu.Data, e = ioutil.ReadAll(io.LimitReader(r.Body, sizeLimit+1))
	if e == io.EOF || int64(pu.OriginalDataSize) == sizeLimit+1 {
		io.Copy(ioutil.Discard, r.Body)
	}
	r.Body.Close()
	return nil
}

/// 从 http 请求中 读取 数据
func parseMultipart(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {
	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}
	}()
	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}

	//first multi-part item
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}

	pu.FileName = part.FileName()
	if pu.FileName != "" {
		pu.FileName = path.Base(pu.FileName)
	}

	pu.Data, e = ioutil.ReadAll(io.LimitReader(part, sizeLimit+1))
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}
	if len(pu.Data) == int(sizeLimit)+1 {
		e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
		return
	}

	//if the filename is empty string, do a search on the other multi-part items
	for pu.FileName == "" {
		part2, fe := form.NextPart()
		if fe != nil {
			break // no more or on error, just safely break
		}

		fName := part2.FileName()

		//found the first <file type> multi-part has filename
		if fName != "" {
			data2, fe2 := ioutil.ReadAll(io.LimitReader(part2, sizeLimit+1))
			if fe2 != nil {
				glog.V(0).Infoln("Reading Content [ERROR]", fe2)
				e = fe2
				return
			}
			if len(data2) == int(sizeLimit)+1 {
				e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
				return
			}

			//update
			pu.Data = data2
			pu.FileName = path.Base(fName)
			break
		}
	}

	pu.IsChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !pu.IsChunkedFile {

		dotIndex := strings.LastIndex(pu.FileName, ".")
		ext, mtype := "", ""
		if dotIndex > 0 {
			ext = strings.ToLower(pu.FileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType := part.Header.Get("Content-Type")
		if contentType != "" && contentType != "application/octet-stream" && mtype != contentType {
			pu.MimeType = contentType //only return mime type if not deductable
			mtype = contentType
		}

		pu.IsGzipped = part.Header.Get("Content-Encoding") == "gzip"
	}

	return
}
