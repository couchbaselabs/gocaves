package mock

import (
	"github.com/couchbase/gocbcore/v9/memd"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(hooks *KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdGet).Handler(x.handleGetRequest)
}

func (x *kvImplCrud) handleGetRequest(source *KvClient, pak *memd.Packet, next func()) {
	next()
}
