package needle

import (
	"encoding/hex"
	"fmt"
	"strings"

	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

/// volume 下 的 needle file id, volume id 实际是 32 位无符号整型, needle id 和 cookie 分别是 无符号的 64 位 和 32 位 整数
type FileId struct {
	VolumeId VolumeId
	Key      NeedleId
	Cookie   Cookie
}

func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Cookie: n.Cookie}
}

func NewFileId(VolumeId VolumeId, key uint64, cookie uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Uint64ToNeedleId(key), Cookie: Uint32ToCookie(cookie)}
}

// Deserialize the file id
func ParseFileIdFromString(fid string) (*FileId, error) {
	vid, needleKeyCookie, err := splitVolumeId(fid)
	if err != nil {
		return nil, err
	}
	/// volume id 实际是 32 位无符号整型
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		return nil, err
	}

	/// needle id 和 cookie 分别是 无符号的 64 位 和 32 位 整数
	nid, cookie, err := ParseNeedleIdCookie(needleKeyCookie)
	if err != nil {
		return nil, err
	}
	fileId := &FileId{VolumeId: volumeId, Key: nid, Cookie: cookie}
	return fileId, nil
}

func (n *FileId) GetVolumeId() VolumeId {
	return n.VolumeId
}

func (n *FileId) GetNeedleId() NeedleId {
	return n.Key
}

func (n *FileId) GetCookie() Cookie {
	return n.Cookie
}

func (n *FileId) GetNeedleIdCookie() string {
	return formatNeedleIdCookie(n.Key, n.Cookie)
}

func (n *FileId) String() string {
	return n.VolumeId.String() + "," + formatNeedleIdCookie(n.Key, n.Cookie)
}

func formatNeedleIdCookie(key NeedleId, cookie Cookie) string {
	bytes := make([]byte, NeedleIdSize+CookieSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	CookieToBytes(bytes[NeedleIdSize:NeedleIdSize+CookieSize], cookie)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return hex.EncodeToString(bytes[nonzero_index:])
}

// copied from operation/delete_content.go, to cut off cycle dependency
/// 形如 逗号分隔的 [volume id,needle id and cookie]
func splitVolumeId(fid string) (vid string, key_cookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", fmt.Errorf("wrong fid format")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}
