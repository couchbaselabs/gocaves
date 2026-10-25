package mockimpl

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

// TestCommandValidators checks the checks kv_engine's validators make after
// verify_header (daemon/mcbp_validators.cc@v7.0.5), and that the requests they
// accept are served.
func TestCommandValidators(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureJSON, memd.FeatureSnappy)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
	uuid := make([]byte, 8)

	for _, test := range []struct {
		name    string
		pak     memd.Packet
		status  memd.StatusCode
		context string
	}{
		{name: "GET_LOCKED without extras", pak: memd.Packet{Command: memd.CmdGetLocked, Key: []byte("doc")}, status: memd.StatusSuccess},
		{name: "GET_LOCKED with 1 byte of extras", pak: memd.Packet{Command: memd.CmdGetLocked, Key: []byte("doc"), Extras: []byte{1}},
			status: memd.StatusInvalidArgs, context: "Request extras must be of length 0 or 4"},
		{name: "GET_META with 2 bytes of extras", pak: memd.Packet{Command: memd.CmdGetMeta, Key: []byte("doc"), Extras: []byte{1, 2}},
			status: memd.StatusInvalidArgs, context: "Request extras must be of length 0 or 1"},
		{name: "GET_META version 3", pak: memd.Packet{Command: memd.CmdGetMeta, Key: []byte("doc"), Extras: []byte{3}},
			status: memd.StatusInvalidArgs, context: "Request extras invalid"},
		{name: "HELLO with a value of odd length", pak: memd.Packet{Command: memd.CmdHello, Value: []byte{0}},
			status: memd.StatusInvalidArgs, context: "Request value must be of even length"},
		{name: "STAT with a value not JSON", pak: memd.Packet{Command: memd.CmdStat, Value: []byte("x")},
			status: memd.StatusInvalidArgs, context: "Datatype must be JSON"},
		{name: "STAT with an invalid JSON value", pak: memd.Packet{Command: memd.CmdStat, Value: []byte("{"), Datatype: uint8(memd.DatatypeFlagJSON)},
			status: memd.StatusInvalidArgs, context: "value is not valid JSON"},
		{name: "OBSERVE_SEQNO with 9 bytes", pak: memd.Packet{Command: memd.CmdObserveSeqNo, Value: make([]byte, 9)},
			status: memd.StatusInvalidArgs, context: "Request value must be of length 8"},
		{name: "OBSERVE_SEQNO of an unknown vbucket UUID", pak: memd.Packet{Command: memd.CmdObserveSeqNo, Value: uuid},
			status: memd.StatusKeyNotFound},
		{name: "GET_ERROR_MAP of vbucket 1", pak: memd.Packet{Command: memd.CmdGetErrorMap, Vbucket: 1, Value: []byte{0, 2}},
			status: memd.StatusInvalidArgs, context: "Request vbucket id must be 0"},
		{name: "GET_ERROR_MAP with 1 byte", pak: memd.Packet{Command: memd.CmdGetErrorMap, Value: []byte{2}},
			status: memd.StatusInvalidArgs, context: "Request value must be of length 2"},
		{name: "GET_ERROR_MAP version 2", pak: memd.Packet{Command: memd.CmdGetErrorMap, Value: []byte{0, 2}}, status: memd.StatusSuccess},
		{name: "SET that does not inflate", pak: memd.Packet{Command: memd.CmdSet, Key: []byte("doc"), Extras: make([]byte, 8), Value: []byte{0xff},
			Datatype: uint8(memd.DatatypeFlagCompressed)}, status: memd.StatusInvalidArgs, context: "Failed to inflate payload"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pak := test.pak
			if pak.Command != memd.CmdGetErrorMap {
				pak.Vbucket = c.vbID
			}
			resp := c.Do(&pak)
			if resp.Status != test.status {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(test.status), uint16(resp.Status))
			}
			if test.context != "" {
				expectErrorContext(t, resp, test.context)
			}
			if pak.Command == memd.CmdGetLocked && resp.Status == memd.StatusSuccess {
				c.mustSucceed(&memd.Packet{Command: memd.CmdUnlockKey, Vbucket: c.vbID, Key: []byte("doc"), Cas: resp.Cas})
			}
		})
	}
}

// TestObserveSeqNoOfEarlierHistory checks that OBSERVE_SEQNO of the vbucket
// UUID of an earlier history answers the failover format, with the latest
// UUID and the sequence number where the earlier history ended, as
// EventuallyPersistentEngine::observe_seqno does.
func TestObserveSeqNoOfEarlierHistory(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSeqNo)
	set := c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("a"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	oldUUID, oldSeqNo := set.Extras[0:8], binary.BigEndian.Uint64(set.Extras[8:])

	store := c.cluster.GetBucket("default").Store()
	if err := store.Rollback(store.Snapshot()); err != nil {
		t.Fatalf("failed to roll back: %v", err)
	}
	set = c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("b"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	newUUID := set.Extras[0:8]

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdObserveSeqNo, Vbucket: c.vbID, Value: oldUUID})
	if len(resp.Value) != 43 || resp.Value[0] != 1 {
		t.Fatalf("expected the failover format, got %v", resp.Value)
	}
	if !bytes.Equal(resp.Value[3:11], newUUID) || !bytes.Equal(resp.Value[27:35], oldUUID) || binary.BigEndian.Uint64(resp.Value[35:]) != oldSeqNo {
		t.Errorf("expected latest UUID %x, old UUID %x ending at %d, got %v", newUUID, oldUUID, oldSeqNo, resp.Value)
	}
}

// TestNotMyVbucketCarriesConfig checks that not_my_vbucket carries the
// bucket's cluster configuration, as kv_engine's Cookie::sendNotMyVBucket
// sends it while dedupe_nmvb_maps is off, its default, with the JSON
// datatype even to a client that did not enable JSON with HELLO.
func TestNotMyVbucketCarriesConfig(t *testing.T) {
	c := newTestKvConn(t)
	bucket := c.cluster.GetBucket("default")

	for _, req := range []*memd.Packet{
		{Command: memd.CmdGet, Vbucket: vbucketOwnedAs(t, c, bucket, 1), Key: []byte("doc")},
		{Command: memd.CmdGetReplica, Vbucket: c.vbID, Key: []byte("doc")},
	} {
		command := req.Command
		resp := c.Do(req)
		if resp.Status != memd.StatusNotMyVBucket {
			t.Fatalf("%s: expected status 0x%02x, got 0x%02x", command.Name(), uint16(memd.StatusNotMyVBucket), uint16(resp.Status))
		}
		var config struct {
			Rev    *int            `json:"rev"`
			VBSMap json.RawMessage `json:"vBucketServerMap"`
		}
		if err := json.Unmarshal(resp.Value, &config); err != nil || config.Rev == nil || config.VBSMap == nil || resp.Datatype != uint8(memd.DatatypeFlagJSON) {
			t.Errorf("%s: expected the cluster configuration as JSON, got %d bytes with datatype 0x%02x (%v)", command.Name(), len(resp.Value), resp.Datatype, err)
		}
	}
}
