package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

/// 将 lastTimestampNs 纳秒以后的 needle 全部发送, 并且进行 类 tail 方式 等待一段时间 再发送
func (vs *VolumeServer) VolumeTailSender(req *volume_server_pb.VolumeTailSenderRequest, stream volume_server_pb.VolumeServer_VolumeTailSenderServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("tailing volume %d finished", v.Id)

	lastTimestampNs := req.SinceNs
	drainingSeconds := req.IdleTimeoutSeconds

	for {
		/// 将 lastTimestampNs 纳秒以后的 needle 全部发送
		lastProcessedTimestampNs, err := sendNeedlesSince(stream, v, lastTimestampNs)
		if err != nil {
			glog.Infof("sendNeedlesSince: %v", err)
			return fmt.Errorf("streamFollow: %v", err)
		}
		time.Sleep(2 * time.Second)

		if req.IdleTimeoutSeconds == 0 {
			lastTimestampNs = lastProcessedTimestampNs
			continue
		}
		if lastProcessedTimestampNs == lastTimestampNs {
			drainingSeconds--
			if drainingSeconds <= 0 {
				return nil
			}
			glog.V(1).Infof("tailing volume %d drains requests with %d seconds remaining", v.Id, drainingSeconds)
		} else {
			lastTimestampNs = lastProcessedTimestampNs
			drainingSeconds = req.IdleTimeoutSeconds
			glog.V(1).Infof("tailing volume %d resets draining wait time to %d seconds", v.Id, drainingSeconds)
		}

	}

}

/// 将 lastTimestampNs 纳秒以后的 needle 全部发送
func sendNeedlesSince(stream volume_server_pb.VolumeServer_VolumeTailSenderServer, v *storage.Volume, lastTimestampNs uint64) (lastProcessedTimestampNs uint64, err error) {

	/// 从 sinceNs 纳秒 处 开始, 通过 index file 读取 1 个 needle 在 volume 的 offset
	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(lastTimestampNs)
	if err != nil {
		return 0, fmt.Errorf("fail to locate by appendAtNs %d: %s", lastTimestampNs, err)
	}

	// log.Printf("reading ts %d offset %d isLast %v", lastTimestampNs, foundOffset, isLastOne)

	/// 是最后一个
	if isLastOne {
		// need to heart beat to the client to ensure the connection health
		sendErr := stream.Send(&volume_server_pb.VolumeTailSenderResponse{IsLastChunk: true})
		return lastTimestampNs, sendErr
	}

	/// 调用 scanner 中的 send 函数 将 needle header 和 body 发送
	scanner := &VolumeFileScanner4Tailing{
		stream: stream,
	}

	/// 不是最后一个
	/// 循环扫描 volume 中的 Needle 文件, 头部 和 数据体 保存在 backend 中, 并且调用 scanner 的 VisitNeedle
	err = storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, foundOffset.ToAcutalOffset(), scanner)

	return scanner.lastProcessedTimestampNs, err

}

/// 从 目标 volume server 上用类似 tail 的方式进行拷贝 needle , 只拷贝 sinceNs 纳秒后添加的, 并将数据写入本地
/// volume server 上的实际处理函数 是 VolumeTailSender
func (vs *VolumeServer) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest) (*volume_server_pb.VolumeTailReceiverResponse, error) {

	resp := &volume_server_pb.VolumeTailReceiverResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return resp, fmt.Errorf("receiver not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("receive tailing volume %d finished", v.Id)

	/// 从 目标 volume server 上用类似 tail 的方式进行拷贝 needle , 只拷贝 sinceNs 纳秒后添加的, 并调用 fn 处理 needle
	/// 即将 volume 写入本地
	return resp, operation.TailVolumeFromSource(req.SourceVolumeServer, vs.grpcDialOption, v.Id, req.SinceNs, int(req.IdleTimeoutSeconds), func(n *needle.Needle) error {
		_, err := vs.store.WriteVolumeNeedle(v.Id, n, false)
		return err
	})

}

// generate the volume idx
type VolumeFileScanner4Tailing struct {
	stream                   volume_server_pb.VolumeServer_VolumeTailSenderServer
	lastProcessedTimestampNs uint64
}

func (scanner *VolumeFileScanner4Tailing) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}
func (scanner *VolumeFileScanner4Tailing) ReadNeedleBody() bool {
	return true
}

/// 调用 scanner 中的 send 函数 将 needle header 和 body 发送
func (scanner *VolumeFileScanner4Tailing) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	isLastChunk := false

	// need to send body by chunks
	for i := 0; i < len(needleBody); i += BufferSizeLimit {
		stopOffset := i + BufferSizeLimit
		if stopOffset >= len(needleBody) {
			isLastChunk = true
			stopOffset = len(needleBody)
		}

		sendErr := scanner.stream.Send(&volume_server_pb.VolumeTailSenderResponse{
			NeedleHeader: needleHeader,
			NeedleBody:   needleBody[i:stopOffset],
			IsLastChunk:  isLastChunk,
		})
		if sendErr != nil {
			return sendErr
		}
	}

	scanner.lastProcessedTimestampNs = n.AppendAtNs
	return nil
}
