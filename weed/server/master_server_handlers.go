package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

/// 获取各个 volume id 的 location 信息, 从 map 中获取
func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]operation.LookupResult) {
	volumeLocations = make(map[string]operation.LookupResult)
	for _, vid := range vids {
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if _, ok := volumeLocations[vid]; ok {
			continue
		}
		volumeLocations[vid] = ms.findVolumeLocation(collection, vid)
	}
	return
}

/// 根据 collection 和 volume id 获取节点位置, 从 map 中获取
// If "fileId" is provided, this returns the fileId location and a JWT to update or delete the file.
// If "volumeId" is provided, this only returns the volumeId location
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	if vid != "" {
		// backward compatible
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
	}
	fileId := r.FormValue("fileId")
	if fileId != "" {
		commaSep := strings.Index(fileId, ",")
		if commaSep > 0 {
			vid = fileId[0:commaSep]
		}
	}
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	location := ms.findVolumeLocation(collection, vid)
	httpStatus := http.StatusOK
	if location.Error != "" || location.Locations == nil {
		httpStatus = http.StatusNotFound
	} else {
		forRead := r.FormValue("read")
		isRead := forRead == "yes"
		ms.maybeAddJwtAuthorization(w, fileId, !isRead)
	}
	writeJsonQuiet(w, r, httpStatus, location)
}

/// 根据 collection 和 volume id 查找位置
/// 如果是 master 则从本地内存中 中读取, 否则 从 master client 中读取, 而 master client 中的数据是 通过 master 向 leader master
/// 发送 KeepConnected 请求保持连接, 实时获取 vid 信息的.
// findVolumeLocation finds the volume location from master topo if it is leader,
// or from master client if not leader
func (ms *MasterServer) findVolumeLocation(collection, vid string) operation.LookupResult {
	var locations []operation.Location
	var err error
	/// 如果是 leader master
	if ms.Topo.IsLeader() {
		volumeId, newVolumeIdErr := needle.NewVolumeId(vid)
		if newVolumeIdErr != nil {
			err = fmt.Errorf("Unknown volume id %s", vid)
		} else {
			/// 查找 特定 volume id 的位置, 会递归调用 Collection 和 VolumeLayout 的 Lookup 方法, 都是从 map 中获取
			machines := ms.Topo.Lookup(collection, volumeId)
			for _, loc := range machines {
				locations = append(locations, operation.Location{Url: loc.Url(), PublicUrl: loc.PublicUrl})
			}
			if locations == nil {
				err = fmt.Errorf("volume id %s not found", vid)
			}
		}
	} else {
		/// 从 vidMap 中获取数据, 当中的数据是 master 之间 KeepConnected 获取的
		machines, getVidLocationsErr := ms.MasterClient.GetVidLocations(vid)
		for _, loc := range machines {
			locations = append(locations, operation.Location{Url: loc.Url, PublicUrl: loc.PublicUrl})
		}
		err = getVidLocationsErr
	}
	ret := operation.LookupResult{
		VolumeId:  vid,
		Locations: locations,
	}
	if err != nil {
		ret.Error = err.Error()
	}
	return ret
}

/// 分配节点 用于存储数据
func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	writableVolumeCount, e := strconv.Atoi(r.FormValue("writableVolumeCount"))
	if e != nil {
		writableVolumeCount = 0
	}

	/// 获取 topology.VolumeGrowOption 里面包含数据中心、机架、节点等信息
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	/// 如果对应数据中心\机架\节点没有可写的volume, 实际是从 volume layout 的 writable 表中查看并获取对应的 数据 中心 和 机架 是否有空闲节点
	if !ms.Topo.HasWritableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			writeJsonQuiet(w, r, http.StatusNotFound, operation.AssignResult{Error: "No free volumes left!"})
			return
		}
		ms.vgLock.Lock()
		defer ms.vgLock.Unlock()
		if !ms.Topo.HasWritableVolume(option) {
			/// 如果没有可写节点, 就进行卷 volume 的扩容
			if _, err = ms.vg.AutomaticGrowByType(option, ms.grpcDialOption, ms.Topo, writableVolumeCount); err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Cannot grow volume group! %v", err))
				return
			}
		}
	}
	/// 从 volume layout 中的 可写 列表中 查寻一个满足条件的节点
	fid, count, dn, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		ms.maybeAddJwtAuthorization(w, fid, true)
		writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
	} else {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
	}
}

func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string, isWrite bool) {
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwt(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwt(ms.guard.ReadSigningKey, ms.guard.ReadExpiresAfterSec, fileId)
	}
	if encodedJwt == "" {
		return
	}

	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
