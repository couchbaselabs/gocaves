package svcimpls

import (
	"log"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

func writePacketToSource(source mock.KvClient, pak *memd.Packet, start time.Time) {
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
