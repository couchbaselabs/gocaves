package svcimpls

import (
	"fmt"
	"strings"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
)

// TestKvEngineScopeOrder checks kvEngineScopeOrder against the order in which
// libstdc++'s std::unordered_map<uint32_t, ...> iterates the scope ids 0x8f to
// 8, without multiples of 5, and 0, inserted in that order: the order
// ns_server lists them in once the others are dropped. The map grows to 127
// buckets on the way.
func TestKvEngineScopeOrder(t *testing.T) {
	var scopes []mock.CollectionManifestScope
	for uid := uint32(0x8f); uid >= 8; uid-- {
		if uid%5 != 0 {
			scopes = append(scopes, mock.CollectionManifestScope{UID: uid})
		}
	}
	scopes = append(scopes, mock.CollectionManifestScope{UID: 0})

	var got []string
	for _, scope := range kvEngineScopeOrder(scopes) {
		got = append(got, fmt.Sprintf("%x", scope.UID))
	}
	const expected = "8 d 11 12 13 15 16 17 18 1a 1b 1c 1d 1f 20 21 22 24 25 26 27 29 2a 2b 2c 2e 2f 30 31 33 34 35 36 38 39 3a 3b 3d 3e 3f 40 42 43 44 45 6f 74 75 76 77 79 7a 7b 7c 7e 0 7f 72 10 8f 54 71 8e 53 81 70 e 8d 52 80 c 8b 6d b 8a 4f 6c 89 4e 9 88 4d 86 85 4a 84 49 83 48 6b 6a 68 67 66 65 63 62 61 60 5e 5d 5c 5b 59 58 57 56 51 4c 47"
	if strings.Join(got, " ") != expected {
		t.Fatalf("expected order %s, got %s", expected, strings.Join(got, " "))
	}
}
