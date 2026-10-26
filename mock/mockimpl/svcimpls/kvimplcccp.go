package svcimpls

import (
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"time"
)

type kvImplCccp struct {
}

func (x *kvImplCccp) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdGetClusterConfig, x.handleGetClusterConfigReq)
}

func (x *kvImplCccp) handleGetClusterConfigReq(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// kv_engine's get_cluster_config requires, without a bucket, the global
	// SystemSettings privilege, which ns_server derives from {[pools], read}.
	if source.SelectedBucket() == nil {
		cluster := source.Source().Node().Cluster()
		user := cluster.Users().GetUser(source.AuthenticatedUserName())
		if user == nil || !user.GlobalPrivileges(mock.ClusterResources{Cluster: cluster})["SystemSettings"] {
			writeAccessError(source, pak, "SystemSettings", start)
			return
		}
	}

	selectedBucket := source.SelectedBucket()
	var configBytes []byte
	if selectedBucket == nil {
		// Send a global terse configuration
		configBytes = GenTerseClusterConfig(source.Source().Node().Cluster(), source.Source().Node())
	} else {
		if selectedBucket.BucketType() == mock.BucketTypeMemcached {
			writePacketToSource(source, &memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: memd.CmdGetClusterConfig,
				Opaque:  pak.Opaque,
				Status:  memd.StatusKeyNotFound,
			}, start)
			return
		}
		configBytes = GenTerseBucketConfig(selectedBucket, source.Source().Node())
	}

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdGetClusterConfig,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   configBytes,
	}, start)
}
