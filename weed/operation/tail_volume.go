package operation

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func TailVolume(master string, grpcDialOption grpc.DialOption, vid needle.VolumeId, sinceNs uint64, timeoutSeconds int, fn func(n *needle.Needle) error) error {
	// find volume location, replication, ttl info
	lookup, err := Lookup(master, vid.String())
	if err != nil {
		return fmt.Errorf("look up volume %d: %v", vid, err)
	}
	if len(lookup.Locations) == 0 {
		return fmt.Errorf("unable to locate volume %d", vid)
	}

	volumeServer := lookup.Locations[0].Url

	return TailVolumeFromSource(volumeServer, grpcDialOption, vid, sinceNs, timeoutSeconds, fn)
}

/// 从 目标 volume server 上用类似 tail 的方式进行拷贝 needle , 只拷贝 sinceNs 纳秒后添加的, 并调用 fn 处理 needle
func TailVolumeFromSource(volumeServer string, grpcDialOption grpc.DialOption, vid needle.VolumeId, sinceNs uint64, idleTimeoutSeconds int, fn func(n *needle.Needle) error) error {
	return WithVolumeServerClient(volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		stream, err := client.VolumeTailSender(context.Background(), &volume_server_pb.VolumeTailSenderRequest{
			VolumeId:           uint32(vid),
			SinceNs:            sinceNs,
			IdleTimeoutSeconds: uint32(idleTimeoutSeconds),
		})
		if err != nil {
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			needleHeader := resp.NeedleHeader
			needleBody := resp.NeedleBody

			if len(needleHeader) == 0 {
				continue
			}

			for !resp.IsLastChunk {
				resp, recvErr = stream.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					} else {
						return recvErr
					}
				}
				needleBody = append(needleBody, resp.NeedleBody...)
			}

			n := new(needle.Needle)
			n.ParseNeedleHeader(needleHeader)
			n.ReadNeedleBodyBytes(needleBody, needle.CurrentVersion)

			err = fn(n)

			if err != nil {
				return err
			}

		}
		return nil
	})
}
