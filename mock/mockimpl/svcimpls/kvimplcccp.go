package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"time"
)

type kvImplCccp struct {
}

func (x *kvImplCccp) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdGetClusterConfig, x.handleGetClusterConfigReq)
}

func (x *kvImplCccp) handleGetClusterConfigReq(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if !source.CheckAuthenticated(mockauth.PermissionSettings, pak.CollectionID) {
		// TODO(chvck): CheckAuthenticated needs to change, this could be actually be auth or access error depending on the user
		// access levels.
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGetClusterConfig,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		}, start)
		return
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
