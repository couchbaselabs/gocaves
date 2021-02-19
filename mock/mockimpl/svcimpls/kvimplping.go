package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplPing struct {
}

func (x *kvImplPing) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdNoop, x.handlePingRequest)
}

func (x *kvImplPing) handlePingRequest(source mock.KvClient, pak *memd.Packet) {
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdNoop,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	})
}
