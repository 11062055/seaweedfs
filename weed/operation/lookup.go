package operation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}
type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeId:%s, Locations:%v, Error:%s", lr.VolumeId, lr.Locations, lr.Error)
}

var (
	vc VidCache // caching of volume locations, re-check if after 10 minutes
)

/// 查找 volume 有缓存则用缓存 没有缓存则向 server (是master) 发送请求
/// Volume Server 的 GetOrHeadHandler 中会调用, 当 本地 查不到时 进行 redirect 是调用
func Lookup(server string, vid string) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(vid)
	if cache_err != nil {
		if ret, err = do_lookup(server, vid); err == nil {
			vc.Set(vid, ret.Locations, 10*time.Minute)
		}
	} else {
		ret = &LookupResult{VolumeId: vid, Locations: locations}
	}
	return
}

/// 向 master server 发送请求 进行查找
func do_lookup(server string, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func LookupFileId(server string, fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := Lookup(server, parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", errors.New("File Not Found")
	}
	return "http://" + lookup.Locations[rand.Intn(len(lookup.Locations))].Url + "/" + fileId, nil
}

// LookupVolumeIds find volume locations by cache and actual lookup
/// 从 master 处 查询一批 volume id 的位置
func LookupVolumeIds(server string, grpcDialOption grpc.DialOption, vids []string) (map[string]LookupResult, error) {
	ret := make(map[string]LookupResult)
	var unknown_vids []string

	//check vid cache first
	for _, vid := range vids {
		locations, cache_err := vc.Get(vid)
		if cache_err == nil {
			ret[vid] = LookupResult{VolumeId: vid, Locations: locations}
		} else {
			unknown_vids = append(unknown_vids, vid)
		}
	}
	//return success if all volume ids are known
	if len(unknown_vids) == 0 {
		return ret, nil
	}

	//only query unknown_vids

	err := WithMasterServerClient(server, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		req := &master_pb.LookupVolumeRequest{
			VolumeIds: unknown_vids,
		}
		resp, grpcErr := masterClient.LookupVolume(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		//set newly checked vids to cache
		for _, vidLocations := range resp.VolumeIdLocations {
			var locations []Location
			for _, loc := range vidLocations.Locations {
				locations = append(locations, Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
				})
			}
			if vidLocations.Error != "" {
				vc.Set(vidLocations.VolumeId, locations, 10*time.Minute)
			}
			ret[vidLocations.VolumeId] = LookupResult{
				VolumeId:  vidLocations.VolumeId,
				Locations: locations,
				Error:     vidLocations.Error,
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
