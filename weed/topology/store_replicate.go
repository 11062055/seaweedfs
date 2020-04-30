package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/// 写 本地 和 远端 副本
func ReplicatedWrite(masterNode string, s *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request) (isUnchanged bool, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	// check whether this is a replicated write request
	var remoteLocations []operation.Location
	/// 不是另一个 volume server 发过来的备份
	if r.FormValue("type") != "replicate" {
		/// 获取可写的远端副本位置, 并检测 副本 数 是否 匹配
		// this is the initial request
		remoteLocations, err = getWritableRemoteReplications(s, volumeId, masterNode)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// read fsync value
	fsync := false
	if r.FormValue("fsync") == "true" {
		fsync = true
	}

	if s.GetVolume(volumeId) != nil {
		/// 写入本地一个 volume 中的一个文件 needle, 数据进行 append, 索引信息添加到 KV 存储中
		isUnchanged, err = s.WriteVolumeNeedle(volumeId, n, fsync)
		if err != nil {
			err = fmt.Errorf("failed to write to local disk: %v", err)
			glog.V(0).Infoln(err)
			return
		}
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		/// 写入远端副本
		if err = distributedOperation(remoteLocations, s, func(location operation.Location) error {
			u := url.URL{
				Scheme: "http",
				Host:   location.Url,
				Path:   r.URL.Path,
			}
			q := url.Values{
				"type": {"replicate"},
				"ttl":  {n.Ttl.String()},
			}
			if n.LastModified > 0 {
				q.Set("ts", strconv.FormatUint(n.LastModified, 10))
			}
			if n.IsChunkedManifest() {
				q.Set("cm", "true")
			}
			u.RawQuery = q.Encode()

			pairMap := make(map[string]string)
			if n.HasPairs() {
				tmpMap := make(map[string]string)
				err := json.Unmarshal(n.Pairs, &tmpMap)
				if err != nil {
					glog.V(0).Infoln("Unmarshal pairs error:", err)
				}
				for k, v := range tmpMap {
					pairMap[needle.PairNamePrefix+k] = v
				}
			}

			// volume server do not know about encryption
			/// 上传文件 到 volume server
			_, err := operation.UploadData(u.String(), string(n.Name), false, n.Data, n.IsGzipped(), string(n.Mime), pairMap, jwt)
			return err
		}); err != nil {
			err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			glog.V(0).Infoln(err)
		}
	}
	return
}

/// 删除本地 的 和 副本, 同时 检查 副本 数目 是否 一致
func ReplicatedDelete(masterNode string, store *storage.Store,
	volumeId needle.VolumeId, n *needle.Needle,
	r *http.Request) (size uint32, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	var remoteLocations []operation.Location
	/// 如果 type = replicate 则说明是 另一个 volume 发过来的删除请求
	if r.FormValue("type") != "replicate" {
		/// 从 master 获取 远端 副本的位置, 同时检查 副本数目 是否足够, 以确保彻底 删除
		remoteLocations, err = getWritableRemoteReplications(store, volumeId, masterNode)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	size, err = store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		if err = distributedOperation(remoteLocations, store, func(location operation.Location) error {
			return util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
		}); err != nil {
			size = 0
		}
	}
	return
}

type DistributedOperationResult map[string]error

func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

type RemoteResult struct {
	Host  string
	Error error
}

func distributedOperation(locations []operation.Location, store *storage.Store, op func(location operation.Location) error) error {
	length := len(locations)
	results := make(chan RemoteResult)
	for _, location := range locations {
		go func(location operation.Location, results chan RemoteResult) {
			results <- RemoteResult{location.Url, op(location)}
		}(location, results)
	}
	ret := DistributedOperationResult(make(map[string]error))
	for i := 0; i < length; i++ {
		result := <-results
		ret[result.Host] = result.Error
	}

	return ret.Error()
}

/// 获取可写的远端副本位置, 并检测 副本 数 是否 匹配
func getWritableRemoteReplications(s *storage.Store, volumeId needle.VolumeId, masterNode string) (
	remoteLocations []operation.Location, err error) {

	v := s.GetVolume(volumeId)
	if v != nil && v.ReplicaPlacement.GetCopyCount() == 1 {
		return
	}

	// not on local store, or has replications
	lookupResult, lookupErr := operation.Lookup(masterNode, volumeId.String())
	if lookupErr == nil {
		selfUrl := s.Ip + ":" + strconv.Itoa(s.Port)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				remoteLocations = append(remoteLocations, location)
			}
		}
	} else {
		err = fmt.Errorf("failed to lookup for %d: %v", volumeId, lookupErr)
		return
	}

	if v != nil {
		// has one local and has remote replications
		copyCount := v.ReplicaPlacement.GetCopyCount()
		if len(lookupResult.Locations) < copyCount {
			err = fmt.Errorf("replicating opetations [%d] is less than volume %d replication copy count [%d]",
				len(lookupResult.Locations), volumeId, copyCount)
		}
	}

	return
}
