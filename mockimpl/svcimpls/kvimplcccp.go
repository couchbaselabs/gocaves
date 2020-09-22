package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplCccp struct {
}

func (x *kvImplCccp) Register(hooks *hooks.KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdGetClusterConfig).Handler(x.handleGetClusterConfigReq)
}

func (x *kvImplCccp) handleGetClusterConfigReq(source mock.KvClient, pak *memd.Packet, next func()) {
	selectedBucket := source.SelectedBucket()
	if selectedBucket == nil {
		// Send a global terse configuration
		//TODO(brett19): Implement cluster-level CCCP
		next()
		return
	}

	configBytes := GenTerseBucketConfig(selectedBucket, source.Source().Node())
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdGetClusterConfig,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   configBytes,
	})
}
