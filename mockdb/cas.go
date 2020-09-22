package mockdb

import "sync/atomic"

var globalCasIncr uint64 = 1

func generateNewCas() uint64 {
	return atomic.AddUint64(&globalCasIncr, 1)
}
