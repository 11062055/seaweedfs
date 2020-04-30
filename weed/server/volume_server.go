package weed_server

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type VolumeServer struct {
	SeedMasterNodes []string
	currentMaster   string
	pulseSeconds    int
	dataCenter      string
	rack            string
	store           *storage.Store
	guard           *security.Guard
	grpcDialOption  grpc.DialOption

	needleMapKind           storage.NeedleMapType
	FixJpgOrientation       bool
	ReadRedirect            bool
	compactionBytePerSecond int64
	MetricsAddress          string
	MetricsIntervalSec      int
	fileSizeLimitBytes      int64
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNodes []string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readRedirect bool,
	compactionMBPerSecond int,
	fileSizeLimitMB int,
) *VolumeServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
	enableUiAccess := v.GetBool("access.ui")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	vs := &VolumeServer{
		pulseSeconds:            pulseSeconds,
		dataCenter:              dataCenter,
		rack:                    rack,
		needleMapKind:           needleMapKind,
		FixJpgOrientation:       fixJpgOrientation,
		ReadRedirect:            readRedirect,
		grpcDialOption:          security.LoadClientTLS(util.GetViper(), "grpc.volume"),
		compactionBytePerSecond: int64(compactionMBPerSecond) * 1024 * 1024,
		fileSizeLimitBytes:      int64(fileSizeLimitMB) * 1024 * 1024,
	}
	vs.SeedMasterNodes = masterNodes
	/// 读取 store 信息, 会递归加载 所有目录下的 volume 信息 Store -> DiskLocation -> Volume
	/// 从 .vif 文件中 解析 得到 VolumeInfo
	/// 从 .dat 文件中 加载 得到 各个 needle 数据信息
	/// 从 .dat 文件中读取超级块 信息
	/// 从 .idx 文件中 获取 索引信息, 并保存 在 leveldb 中
	/// 将 .idx 中的文件 的相关 数据 遍历 到 metric 中去
	/// 并获取 最大 的 file key, 统计 所有文件总的 大小, 删除 文件 总的 次数, 删除 文件 总的 大小
	vs.store = storage.NewStore(vs.grpcDialOption, port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	/// 后台的一些处理
	handleStaticResources(adminMux)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		adminMux.HandleFunc("/status", vs.guard.WhiteList(vs.statusHandler))
		/*
			adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
			adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
			adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
		*/
	}
	/// 用于处理内部后端请求 包括 volume 及其 副本的 增 删 查, 图片的 缩放扩大等
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		/// 用于处理内部后端请求 包括 volume 及其 副本的 查找, 图片的 缩放扩大等, 若非本地则返回 跳转
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	/// 循环向 master 发送心跳, 具体内容详见 doHeartbeat
	go vs.heartbeat()
	hostAddress := fmt.Sprintf("%s:%d", ip, port)
	go stats.LoopPushingMetric("volumeServer", hostAddress, stats.VolumeServerGather,
		func() (addr string, intervalSeconds int) {
			return vs.MetricsAddress, vs.MetricsIntervalSec
		})

	return vs
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}
