package weed_server

import (
	"context"

	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

/// leader master 才响应该请求
func (ms *MasterServer) CollectionList(ctx context.Context, req *master_pb.CollectionListRequest) (*master_pb.CollectionListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.CollectionListResponse{}
	/// 从topology的map中获取collection
	collections := ms.Topo.ListCollections(req.IncludeNormalVolumes, req.IncludeEcVolumes)
	for _, c := range collections {
		resp.Collections = append(resp.Collections, &master_pb.Collection{
			Name: c,
		})
	}

	return resp, nil
}

/// 删除 collection, 会通过 protobuffer 将 volume server 的 collection 也删除, 之后还要将 leader master 的 topology 中的 map 删除掉
func (ms *MasterServer) CollectionDelete(ctx context.Context, req *master_pb.CollectionDeleteRequest) (*master_pb.CollectionDeleteResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.CollectionDeleteResponse{}

	err := ms.doDeleteNormalCollection(req.Name)

	if err != nil {
		return nil, err
	}

	err = ms.doDeleteEcCollection(req.Name)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

/// 删除 collection
func (ms *MasterServer) doDeleteNormalCollection(collectionName string) error {

	collection, ok := ms.Topo.FindCollection(collectionName)
	if !ok {
		return nil
	}

	/// 调用 collection 的 ListVolumeServers, 再调用 VolumeLayout 的 ListVolumeServers 在 map 中获取 data node
	for _, server := range collection.ListVolumeServers() {
		/// master server 通过 protobuffer 形式 调用 volume server 的 DeleteCollection
		/// 会直接从硬盘中删除 collection 中所有相关 volume 和 ecvolume 的数据信息
		err := operation.WithVolumeServerClient(server.Url(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			return err
		}
	}

	/// leader master 的 topology 中也要从 map 中删除 collection
	ms.Topo.DeleteCollection(collectionName)

	return nil
}

/// 删除 ec collection, 同 doDeleteNormalCollection
func (ms *MasterServer) doDeleteEcCollection(collectionName string) error {

	listOfEcServers := ms.Topo.ListEcServersByCollection(collectionName)

	for _, server := range listOfEcServers {
		err := operation.WithVolumeServerClient(server, ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collectionName,
			})
			return deleteErr
		})
		if err != nil {
			return err
		}
	}

	ms.Topo.DeleteEcCollection(collectionName)

	return nil
}
