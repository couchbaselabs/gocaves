package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplErrMap struct {
}

func (x *kvImplErrMap) Register(hooks *hooks.KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdGetErrorMap).Handler(x.handleErrorMapReq)
}

func (x *kvImplErrMap) handleErrorMapReq(source mock.KvClient, pak *memd.Packet, next func()) {
	errMap := source.Source().Node().ErrorMap()

	b, err := errMap.Marshal()
	if err != nil {
		replyWithError(source, pak, err)
		return
	}

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdGetErrorMap,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   b,
	})
}
