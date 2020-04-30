package needle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/images"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

const (
	NeedleChecksumSize = 4
	PairNamePrefix     = "Seaweed-"
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Cookie Cookie   `comment:"random number to mitigate brute force lookups"`
	Id     NeedleId `comment:"needle id"`
	Size   uint32   `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`

	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	PairsSize    uint16 //version2
	Pairs        []byte `comment:"additional name value pairs, json format, maximum 64kB"`
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *TTL

	Checksum   CRC    `comment:"CRC32 to check integrity"`
	AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3
	Padding    []byte `comment:"Aligned to 8 bytes"`
}

func (n *Needle) String() (str string) {
	str = fmt.Sprintf("%s Size:%d, DataSize:%d, Name:%s, Mime:%s", formatNeedleIdCookie(n.Id, n.Cookie), n.Size, n.DataSize, n.Name, n.Mime)
	return
}

/// 读取 一个 volume needle 数据, 包括 数据 和 附加信息
func CreateNeedleFromRequest(r *http.Request, fixJpgOrientation bool, sizeLimit int64) (n *Needle, originalSize int, e error) {
	n = new(Needle)
	/// 从 http 请求中 读取 数据
	pu, e := ParseUpload(r, sizeLimit)
	if e != nil {
		return
	}
	n.Data = pu.Data
	originalSize = pu.OriginalDataSize
	n.LastModified = pu.ModifiedTime
	n.Ttl = pu.Ttl

	if len(pu.FileName) < 256 {
		n.Name = []byte(pu.FileName)
		n.SetHasName()
	}
	if len(pu.MimeType) < 256 {
		n.Mime = []byte(pu.MimeType)
		n.SetHasMime()
	}
	if len(pu.PairMap) != 0 {
		trimmedPairMap := make(map[string]string)
		for k, v := range pu.PairMap {
			trimmedPairMap[k[len(PairNamePrefix):]] = v
		}

		pairs, _ := json.Marshal(trimmedPairMap)
		if len(pairs) < 65536 {
			n.Pairs = pairs
			n.PairsSize = uint16(len(pairs))
			n.SetHasPairs()
		}
	}
	if pu.IsGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	if pu.IsChunkedFile {
		n.SetIsChunkManifest()
	}

	if fixJpgOrientation {
		loweredName := strings.ToLower(pu.FileName)
		if pu.MimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}

	n.Checksum = NewCRC(n.Data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)

	return
}
/// fid 的格式 fid_delta
func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	if length <= CookieSize*2 {
		return fmt.Errorf("Invalid fid: %s", fid)
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	/// [NeedleId(8字节)][Cookie(8字节)]
	n.Id, n.Cookie, err = ParseNeedleIdCookie(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += Uint64ToNeedleId(d)
		} else {
			return e
		}
	}
	return err
}

/// 从 字符串 中反解出 needle id 和 cookie ,CookieSize = 4, 最后 8 个字节 是 cookie
/// needle id 和 cookie 实际是 64 位 和 32 位 的 无符号 整型
/// [NeedleId(8字节)][Cookie(8字节)]
func ParseNeedleIdCookie(key_hash_string string) (NeedleId, Cookie, error) {
	if len(key_hash_string) <= CookieSize*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too short.")
	}
	if len(key_hash_string) > (NeedleIdSize+CookieSize)*2 {
		return NeedleIdEmpty, 0, fmt.Errorf("KeyHash is too long.")
	}
	split := len(key_hash_string) - CookieSize*2
	/// needle id 实际是 64 位 无符号 整型
	needleId, err := ParseNeedleId(key_hash_string[:split])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse needleId error: %v", err)
	}
	cookie, err := ParseCookie(key_hash_string[split:])
	if err != nil {
		return NeedleIdEmpty, 0, fmt.Errorf("Parse cookie error: %v", err)
	}
	return needleId, cookie, nil
}

func (n *Needle) LastModifiedString() string {
	return time.Unix(int64(n.LastModified), 0).Format("2006-01-02T15:04:05")
}
