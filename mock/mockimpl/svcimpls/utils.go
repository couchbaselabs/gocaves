package svcimpls

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

func writePacketToSource(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if source.HasFeature(memd.FeatureDurations) {
		// TODO (chvck): revisit this, for some reason Windows reports a server duration of 0.
		// Golang time accuracy in Windows is good enough that this shouldn't be the case and it seems pretty unlikely
		// that we've actually done the operation in no time.
		duration := time.Since(start)
		if duration < 1*time.Microsecond {
			duration = time.Duration(rand.Intn(200)+1) * time.Microsecond
		}
		pak.ServerDurationFrame = &memd.ServerDurationFrame{
			ServerDuration: duration,
		}
	}
	err := source.WritePacket(pak)
	if err != nil {
		log.Printf("failed to write packet %+v to %+v", pak, source)
	}
}

func setDatatypeJSONFromValue(docValue []byte) uint8 {
	var docBody interface{}
	err := json.Unmarshal(docValue, &docBody)
	if err == nil {
		return uint8(memd.DatatypeFlagJSON)
	}
	return 0
}
