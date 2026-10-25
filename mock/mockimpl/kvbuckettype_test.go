package mockimpl

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// bucketTypeConn returns a connection to the default EP bucket or, for
// ephemeral, to a new ephemeral bucket, and an active vbucket of it.
func bucketTypeConn(t *testing.T, ephemeral bool, features ...memd.HelloFeature) (*testKvConn, uint16) {
	c := newTestKvConn(t, features...)
	if !ephemeral {
		return c, c.vbID
	}
	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "ephemeral", Type: mock.BucketTypeEphemeral})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("ephemeral")})
	return c, vbucketOwnedAs(t, c, bucket, 0)
}

// getMeta returns the deleted flag, expiry and revision GET_META reports.
func getMeta(t *testing.T, c *testKvConn, vbID uint16, key string) (bool, uint32, uint64) {
	t.Helper()
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: vbID, Key: []byte(key)})
	return binary.BigEndian.Uint32(resp.Extras) != 0, binary.BigEndian.Uint32(resp.Extras[8:]), binary.BigEndian.Uint64(resp.Extras[12:])
}

// TestRevisionOfNewKey checks the revision kv_engine gives a document: the
// next one of the document it replaces while it holds that in memory, or
// else one more than the highest it counts among the deleted documents of
// the vbucket (VBucket::updateRevSeqNoOfNewStoredValue). It counts the
// revision of a deletion DELETE makes, and one more than that of a document
// that expires (HashTable::updateMaxDeletedRevSeqno). An EP bucket drops a
// persisted deletion from memory until a read of it brings it back, as the
// Add of a deleted document does (VBucket::processAdd); an ephemeral bucket
// keeps it.
func TestRevisionOfNewKey(t *testing.T) {
	for _, ephemeral := range []bool{false, true} {
		name := "couchbase"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			c, vbID := bucketTypeConn(t, ephemeral, memd.FeatureXattr)
			set := func(key string, expiry uint32) {
				extras := make([]byte, 8)
				binary.BigEndian.PutUint32(extras[4:], expiry)
				c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte(key), Value: []byte("1"), Extras: extras})
			}
			del := func(key string) {
				c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: vbID, Key: []byte(key)})
			}
			expect := func(what, key string, want uint64) {
				t.Helper()
				if _, _, revID := getMeta(t, c, vbID, key); revID != want {
					t.Errorf("%s: expected revision %d, got %d", what, want, revID)
				}
			}

			for i := 0; i < 5; i++ {
				set("m", 0)
			}
			del("m")
			expect("deleted after 5 sets", "m", 6)
			set("n", 0)
			expect("new key", "n", 7)
			for _, key := range []string{"p", "q", "r", "u"} {
				set(key, 0)
				del(key)
			}
			set("u", 0)
			expect("over a deletion in memory", "u", 15)
			createDeleted := func() memd.StatusCode {
				return c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: vbID, Key: []byte("t"),
					Extras: docFlags(memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagCreateAsDeleted), Value: encodeMutationSpecs(xattrSpec("_s", "1"))}).Status
			}
			if status := createDeleted(); status != memd.StatusSubDocSuccessDeleted {
				t.Fatalf("create deleted: expected status 0x%02x, got 0x%02x", uint16(memd.StatusSubDocSuccessDeleted), uint16(status))
			}
			expect("created deleted", "t", 15)

			c.cluster.Chrono().TimeTravel(2 * time.Second)
			set("p", 0)
			if ephemeral {
				expect("over a deletion an ephemeral bucket keeps", "p", 9)
			} else {
				expect("over a persisted deletion", "p", 15)
			}
			resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: vbID, Key: []byte("q"),
				Extras: docFlags(memd.SubdocDocFlagAccessDeleted), Value: encodeMutationSpecs(xattrSpec("x", "1"))})
			expectStatus(t, "xattr on a deleted document", resp, memd.StatusSubDocSuccessDeleted)
			expect("over a deletion read with AccessDeleted", "q", 11)
			expect("deletion read by GET_META", "r", 12)
			set("r", 0)
			expect("over a deletion read by GET_META", "r", 13)
			// Mkdoc retries the refused Add until it gives up.
			if status := createDeleted(); status != memd.StatusTmpFail {
				t.Errorf("create deleted again: expected status 0x%02x, got 0x%02x", uint16(memd.StatusTmpFail), uint16(status))
			}
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("t"), Value: []byte("1"), Extras: make([]byte, 8)})
			expect("over a deletion a refused Add of a deleted document read", "t", 16)

			set("e", 1)
			c.cluster.Chrono().TimeTravel(2 * time.Second)
			c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: vbID, Key: []byte("e")})
			expect("expired", "e", 16)
			set("f", 0)
			expect("new key after an expiry", "f", 18)
		})
	}
}

// TestEphemeralDeletion checks what an ephemeral bucket does without
// persistence: a deletion stays in memory with no expiry, so a document
// revived with PreserveTtl stays live, and every CAS counts as a hybrid
// logical clock, so $document.last_modified is there at once.
func TestEphemeralDeletion(t *testing.T) {
	for _, ephemeral := range []bool{false, true} {
		name := "couchbase"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			c, vbID := bucketTypeConn(t, ephemeral, memd.FeatureXattr, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})

			spec := encodeLookupSpec(memd.SubDocOpGet, "$document")
			spec[1] = uint8(memd.SubdocFlagXattrPath)
			resp := c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiLookup, Vbucket: vbID, Key: []byte("doc"), Value: spec})
			var vattr map[string]json.RawMessage
			if err := json.Unmarshal(resp.Value[6:], &vattr); err != nil {
				t.Fatalf("failed to decode $document: %v", err)
			}
			if _, ok := vattr["last_modified"]; ok != ephemeral {
				t.Errorf("expected last_modified %v before persistence, got %s", ephemeral, resp.Value[6:])
			}

			c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: vbID, Key: []byte("doc")})
			c.cluster.Chrono().TimeTravel(2 * time.Second)
			if _, exptime, _ := getMeta(t, c, vbID, "doc"); (exptime != 0) == ephemeral {
				t.Errorf("expected an expiry %v for the deletion, got %d", !ephemeral, exptime)
			}

			resp = c.Do(&memd.Packet{
				Command:             memd.CmdSubDocMultiMutation,
				Vbucket:             vbID,
				Key:                 []byte("doc"),
				Extras:              docFlags(memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagReviveDocument),
				Value:               encodeMutationSpecs(xattrSpec("_s", "1")),
				PreserveExpiryFrame: &memd.PreserveExpiryFrame{},
			})
			expectStatus(t, "revive", resp, memd.StatusSuccess)
			get := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: vbID, Key: []byte("doc")})
			if (get.Status == memd.StatusSuccess) != ephemeral {
				t.Errorf("expected the revived document live %v, got status 0x%02x", ephemeral, uint16(get.Status))
			}
		})
	}
}

// TestGetLockedTimeout checks that GET_LOCKED locks for getl_default_timeout,
// 15 seconds, when the request gives no time or one above getl_max_timeout,
// 30 seconds (EventuallyPersistentEngine::getLockedInner).
func TestGetLockedTimeout(t *testing.T) {
	for _, test := range []struct {
		name   string
		extras []byte
		locked time.Duration
	}{
		{name: "no time", locked: 15 * time.Second},
		{name: "60 seconds", extras: []byte{0, 0, 0, 60}, locked: 15 * time.Second},
		{name: "30 seconds", extras: []byte{0, 0, 0, 30}, locked: 30 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
			c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("doc"), Extras: test.extras})
			set := func() memd.StatusCode {
				return c.Do(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)}).Status
			}
			c.cluster.Chrono().TimeTravel(test.locked - time.Second)
			if status := set(); status != memd.StatusLocked {
				t.Errorf("expected the document locked a second before %v, got status 0x%02x", test.locked, uint16(status))
			}
			c.cluster.Chrono().TimeTravel(2 * time.Second)
			if status := set(); status != memd.StatusSuccess {
				t.Errorf("expected the document unlocked a second after %v, got status 0x%02x", test.locked, uint16(status))
			}
		})
	}
}
