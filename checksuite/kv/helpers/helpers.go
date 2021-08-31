package helpers

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"log"
)

func CreateErrorMapHook(status memd.StatusCode, errMap []byte) mock.KvHookFunc {
	return func(source mock.KvClient, pak *memd.Packet, next func()) {
		if pak.Command != memd.CmdGetErrorMap {
			next()
			return
		}

		err := source.WritePacket(&memd.Packet{
			Command: pak.Command,
			Magic:   memd.CmdMagicRes,
			Opaque:  pak.Opaque,
			Status:  status,
			Value:   errMap,
		})
		if err != nil {
			log.Printf("failed to write packet %+v to %+v", pak, source)
		}
	}
}
