package weed_server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/raft"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/shell"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

const (
	SequencerType     = "master.sequencer.type"
	SequencerEtcdUrls = "master.sequencer.sequencer_etcd_urls"
)

type MasterOption struct {
	Port                    int
	MetaFolder              string
	VolumeSizeLimitMB       uint
	VolumePreallocate       bool
	PulseSeconds            int
	DefaultReplicaPlacement string
	GarbageThreshold        float64
	WhiteList               []string
	DisableHttp             bool
	MetricsAddress          string
	MetricsIntervalSec      int
}

type MasterServer struct {
	option *MasterOption
	guard  *security.Guard

	preallocateSize int64

	Topo   *topology.Topology
	vg     *topology.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int

	// notifying clients
	clientChansLock sync.RWMutex
	clientChans     map[string]chan *master_pb.VolumeLocation

	grpcDialOption grpc.DialOption

	/// 向 leader master 建立连接的 client
	MasterClient *wdclient.MasterClient
}

func NewMasterServer(r *mux.Router, option *MasterOption, peers []string) *MasterServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	v.SetDefault("master.replication.treat_replication_as_minimums", false)
	replicationAsMin := v.GetBool("master.replication.treat_replication_as_minimums")

	var preallocateSize int64
	if option.VolumePreallocate {
		preallocateSize = int64(option.VolumeSizeLimitMB) * (1 << 20)
	}

	/// 获取 master 之间的 grpc 相关信息 需要 grpc.master.key grpc.master.cert grpc.master.ca
	grpcDialOption := security.LoadClientTLS(v, "grpc.master")
	ms := &MasterServer{
		option:          option,
		preallocateSize: preallocateSize,
		clientChans:     make(map[string]chan *master_pb.VolumeLocation),
		grpcDialOption:  grpcDialOption,
		MasterClient:    wdclient.NewMasterClient(grpcDialOption, "master", 0, peers),
	}
	/// 当代理请求到 master 去时, 该 channel 用于控制最多多少个并发请求可以代理到 master, 防止太多请求打垮 master
	ms.bounedLeaderChan = make(chan int, 16)

	seq := ms.createSequencer(option)
	if nil == seq {
		glog.Fatalf("create sequencer failed.")
	}
	/// 新建 拓扑
	ms.Topo = topology.NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, ms.option.PulseSeconds, replicationAsMin)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", ms.option.VolumeSizeLimitMB, "MB")

	/// 初始化白名单机制
	ms.guard = security.NewGuard(ms.option.WhiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	/// 处理 http 请求
	if !ms.option.DisableHttp {
		handleStaticResources2(r)
		/// 获取各个数据中心\机架\节点\卷的信息
		r.HandleFunc("/", ms.proxyToLeader(ms.uiStatusHandler))
		r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
		r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
		r.HandleFunc("/dir/lookup", ms.guard.WhiteList(ms.dirLookupHandler))
		r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
		r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
		r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
		r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
		r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
		r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
		/*
			r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
			r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
			r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
		*/
		r.HandleFunc("/{fileId}", ms.redirectHandler)
	}

	/// 收集 dead node 和 满 volume, 进行 空洞 压缩, 只有 leader 才执行, 会向各个 volume 发出 Compact 指令, 采用复制算法
	ms.Topo.StartRefreshWritableVolumes(ms.grpcDialOption, ms.option.GarbageThreshold, ms.preallocateSize)

	/// 启动管理脚本
	ms.startAdminScripts()

	return ms
}

/// 启动 raft server, leader master 是保存在 raft 中的
func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
		}
	})
	ms.Topo.RaftServer.AddEventListener(raft.StateChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("state change: %+v", e)
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", "I am the leader!")
	} else {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "is the leader.")
		}
	}
}

/// 通过 raft 发现 leader master, 代理到 leader master 去处理请求
func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else if ms.Topo.RaftServer != nil && ms.Topo.RaftServer.Leader() != "" {
			/// 防止过多请求打到 leader 中去了
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.RaftServer.Leader())
			if err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", ms.Topo.RaftServer.Leader(), err))
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
			/// 将请求代理到 leader 中去
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			director := proxy.Director
			proxy.Director = func(req *http.Request) {
				actualHost, err := security.GetActualRemoteHost(req)
				if err == nil {
					req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
				}
				director(req)
			}
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			// drop it to the floor
			// writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}

func (ms *MasterServer) startAdminScripts() {
	var err error

	v := util.GetViper()
	adminScripts := v.GetString("master.maintenance.scripts")
	glog.V(0).Infof("adminScripts:\n%v", adminScripts)
	if adminScripts == "" {
		return
	}

	v.SetDefault("master.maintenance.sleep_minutes", 17)
	sleepMinutes := v.GetInt("master.maintenance.sleep_minutes")

	v.SetDefault("master.filer.default_filer_url", "http://localhost:8888/")
	filerURL := v.GetString("master.filer.default_filer_url")

	scriptLines := strings.Split(adminScripts, "\n")

	masterAddress := "localhost:" + strconv.Itoa(ms.option.Port)

	var shellOptions shell.ShellOptions
	shellOptions.GrpcDialOption = security.LoadClientTLS(v, "grpc.master")
	shellOptions.Masters = &masterAddress

	shellOptions.FilerHost, shellOptions.FilerPort, shellOptions.Directory, err = util.ParseFilerUrl(filerURL)
	if err != nil {
		glog.V(0).Infof("failed to parse master.filer.default_filer_urll=%s : %v\n", filerURL, err)
		return
	}

	commandEnv := shell.NewCommandEnv(shellOptions)

	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	/// master 之间保持连接 循环 更新 volume id 到 location 之间的对应关系
	go commandEnv.MasterClient.KeepConnectedToMaster()

	go func() {
		/// 阻塞直到 获取到了 master
		commandEnv.MasterClient.WaitUntilConnected()

		c := time.Tick(time.Duration(sleepMinutes) * time.Minute)
		for range c {
			/// master 执行 shell 启动脚本
			if ms.Topo.IsLeader() {
				for _, line := range scriptLines {

					cmds := reg.FindAllString(line, -1)
					if len(cmds) == 0 {
						continue
					}
					args := make([]string, len(cmds[1:]))
					for i := range args {
						args[i] = strings.Trim(string(cmds[1+i]), "\"'")
					}
					cmd := strings.ToLower(cmds[0])

					for _, c := range shell.Commands {
						if c.Name() == cmd {
							glog.V(0).Infof("executing: %s %v", cmd, args)
							if err := c.Do(args, commandEnv, os.Stdout); err != nil {
								glog.V(0).Infof("error: %v", err)
							}
						}
					}
				}
			}
		}
	}()
}

/// 序列号生成器
func (ms *MasterServer) createSequencer(option *MasterOption) sequence.Sequencer {
	var seq sequence.Sequencer
	v := util.GetViper()
	seqType := strings.ToLower(v.GetString(SequencerType))
	glog.V(1).Infof("[%s] : [%s]", SequencerType, seqType)
	switch strings.ToLower(seqType) {
	case "etcd":
		var err error
		urls := v.GetString(SequencerEtcdUrls)
		glog.V(0).Infof("[%s] : [%s]", SequencerEtcdUrls, urls)
		seq, err = sequence.NewEtcdSequencer(urls, option.MetaFolder)
		if err != nil {
			glog.Error(err)
			seq = nil
		}
	default:
		seq = sequence.NewMemorySequencer()
	}
	return seq
}
