package weed_server

import (
	"net/http"

	"github.com/chrislusf/raft"
	ui "github.com/chrislusf/seaweedfs/weed/server/master_ui"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	args := struct {
		Version    string
		Topology   interface{}
		RaftServer raft.Server
		Stats      map[string]interface{}
		Counters   *stats.ServerStats
	}{
		util.VERSION,
		/// 会递归调用 各个 data center \ rack \ data node 的 ToMap 方法, 获取统计信息
		ms.Topo.ToMap(),
		ms.Topo.RaftServer,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
