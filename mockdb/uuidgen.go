package mockdb

import "sync/atomic"

// This ends with CA to make it easier to see it's a CAS.
var globalCasIncr uint64 = 1

func generateNewCas() uint64 {
	newVal := atomic.AddUint64(&globalCasIncr, 1)
	return newVal<<8 | 0xCA
}

// This ends with AF to make it easier to see its a VbUUID.
var globalVbUUIDIncr uint64 = 1

func generateNewVbUUID() uint64 {
	newVal := atomic.AddUint64(&globalVbUUIDIncr, 1)
	return newVal<<8 | 0xAF
}
