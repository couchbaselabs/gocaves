package mockdb

import (
	"sync/atomic"
	"time"
)

// This ends with CA to make it easier to see it's a CAS.
var globalCasIncr uint64 = 1

// GenerateNewCas will generate a new CAS to use with a document.
func GenerateNewCas(now time.Time) uint64 {
	clockTime := uint64(now.UnixNano()) & 0xFFFFFFFFFFFF0000
	logicalTime := atomic.AddUint64(&globalCasIncr, 1) & 0x000000000000FF00
	mockTime := uint64(0x00000000000000CA)

	return clockTime | logicalTime | mockTime
}

// This ends with AF to make it easier to see its a VbUUID.
var globalVbUUIDIncr uint64 = 1

func generateNewVbUUID() uint64 {
	newVal := atomic.AddUint64(&globalVbUUIDIncr, 1)
	return newVal<<8 | 0xAF
}
