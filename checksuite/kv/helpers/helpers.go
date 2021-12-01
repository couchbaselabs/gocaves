package helpers

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"log"
	"time"
)

func CreateErrorMapHook(status memd.StatusCode, errMap []byte) mock.KvHookFunc {
	return func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command != memd.CmdGetErrorMap {
			next()
			return
		}

		WritePacketToSource(source, &memd.Packet{
			Command: pak.Command,
			Magic:   memd.CmdMagicRes,
			Opaque:  pak.Opaque,
			Status:  status,
			Value:   errMap,
		}, start)
	}
}

func WritePacketToSource(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if source.HasFeature(memd.FeatureDurations) {
		pak.ServerDurationFrame = &memd.ServerDurationFrame{
			ServerDuration: time.Since(start),
		}
	}
	err := source.WritePacket(pak)
	if err != nil {
		log.Printf("failed to write packet %+v to %+v", pak, source)
	}
}
