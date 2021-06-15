package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"time"
)

type kvImplErrMap struct {
}

func (x *kvImplErrMap) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdGetErrorMap, x.handleErrorMapReq)
}

func (x *kvImplErrMap) handleErrorMapReq(source mock.KvClient, pak *memd.Packet, start time.Time) {
	errMap := source.Source().Node().ErrorMap()

	b, err := errMap.Marshal()
	if err != nil {
		replyWithError(source, pak, start, err)
		return
	}

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdGetErrorMap,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   b,
	}, start)
}
