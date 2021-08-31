package error

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/checks"
	"github.com/couchbaselabs/gocaves/mock"
	"log"
)

// CheckExtendedError confirms that the SDK can successfully read extended error data from a packet value.
func CheckExtendedError(t *checks.T) {
	t.RequireMock()
	handler := func(source mock.KvClient, pak *memd.Packet, next func()) {
		err := source.WritePacket(&memd.Packet{
			Command:  pak.Command,
			Magic:    memd.CmdMagicRes,
			Opaque:   pak.Opaque,
			Status:   memd.StatusKeyNotFound,
			Value:    []byte(`{"error":{"context":"document could not be found","ref":"someref"}}`),
			Datatype: uint8(memd.DatatypeFlagJSON),
		})
		if err != nil {
			log.Printf("failed to write packet %+v to %+v", pak, source)
		}
	}
	col := t.Collection()
	hooks := t.Mock().KvInHooks()
	hooks.Add(col.HookHelper(handler).Cmd(memd.CmdGetLocked).Build())

	col.KvExpectReq().
		Cmd(memd.CmdGetLocked).Wait()
}
