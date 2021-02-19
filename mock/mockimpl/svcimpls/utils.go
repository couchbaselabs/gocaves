package svcimpls

import (
	"log"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

func writePacketToSource(source mock.KvClient, pak *memd.Packet) {
	err := source.WritePacket(pak)
	if err != nil {
		log.Printf("failed to write packet %+v to %+v", pak, source)
	}
}
