package servers

import "github.com/golang/snappy"

// MaxInflatedSize is the largest value kv_engine inflates, its maximum packet
// size. A value that inflates to more than the store's 20 MiB item limit but
// within this one is answered too_big by the store, as kv_engine answers it.
const MaxInflatedSize = maxPacketSize

// InflateValue decompresses a snappy-compressed request value, as kv_engine
// does in verify_header. It refuses a value whose header claims more than
// MaxInflatedSize before the decoder allocates that length.
func InflateValue(value []byte) ([]byte, bool) {
	size, err := snappy.DecodedLen(value)
	if err != nil || size > MaxInflatedSize {
		return nil, false
	}
	inflated, err := snappy.Decode(nil, value)
	if err != nil {
		return nil, false
	}
	return inflated, true
}
