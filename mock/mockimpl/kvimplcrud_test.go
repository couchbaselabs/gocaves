package mockimpl

import (
	"encoding/binary"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestMutationTokenFollowsHello(t *testing.T) {
	for _, test := range []struct {
		name       string
		features   []memd.HelloFeature
		extrasSize int
	}{
		{name: "without seqno", extrasSize: 0},
		{name: "with seqno", features: []memd.HelloFeature{memd.FeatureSeqNo}, extrasSize: 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, test.features...)

			for _, req := range []*memd.Packet{
				{Command: memd.CmdSet, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)},
				{
					Command: memd.CmdSubDocMultiMutation,
					Key:     []byte("doc"),
					Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}),
				},
				{Command: memd.CmdDelete, Key: []byte("doc")},
			} {
				req.Vbucket = c.vbID
				resp := c.mustSucceed(req)
				if len(resp.Extras) != test.extrasSize {
					t.Errorf("%s: expected %d bytes of extras, got %d", req.Command.Name(), test.extrasSize, len(resp.Extras))
				}
			}
		})
	}
}

// kv_engine answers APPEND and PREPEND on a missing or deleted document with
// not_stored rather than key_not_found.
func TestAdjoinToMissingDocumentIsNotStored(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("deleted"), Value: []byte("a"), Extras: make([]byte, 8)})
	c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("deleted")})

	for _, command := range []memd.CmdCode{memd.CmdAppend, memd.CmdPrepend} {
		for _, key := range []string{"missing", "deleted"} {
			resp := c.Do(&memd.Packet{Command: command, Vbucket: c.vbID, Key: []byte(key), Value: []byte("b")})
			if resp.Status != memd.StatusNotStored {
				t.Errorf("%s on a %s document: expected status 0x%02x, got 0x%02x", command.Name(), key, uint16(memd.StatusNotStored), uint16(resp.Status))
			}
		}
	}
}

// kv_engine parses a counter with safe_strtoull and answers a value it cannot
// parse with delta_badval.
func TestCounterValueParsedAsKvEngine(t *testing.T) {
	c := newTestKvConn(t)

	// Delta 1, initial 0, expiry 0xffffffff so a missing counter is not
	// created.
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras, 1)
	binary.BigEndian.PutUint32(extras[16:], 0xffffffff)

	for _, test := range []struct {
		value  string
		status memd.StatusCode
		result uint64
	}{
		{value: "12", result: 13},
		{value: " \t12", result: 13},
		{value: "+12", result: 13},
		{value: "12 trailing", result: 13},
		{value: `"text"`, status: memd.StatusBadDelta},
		{value: "12abc", status: memd.StatusBadDelta},
		{value: "-5", status: memd.StatusBadDelta},
		{value: "+-5", status: memd.StatusBadDelta},
		{value: "", status: memd.StatusBadDelta},
		{value: "18446744073709551616", status: memd.StatusBadDelta},
	} {
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("counter"), Value: []byte(test.value), Extras: make([]byte, 8)})
		resp := c.Do(&memd.Packet{Command: memd.CmdIncrement, Vbucket: c.vbID, Key: []byte("counter"), Extras: extras})
		if resp.Status != test.status {
			t.Errorf("INCREMENT of %q: expected status 0x%02x, got 0x%02x", test.value, uint16(test.status), uint16(resp.Status))
			continue
		}
		if test.status != memd.StatusSuccess {
			continue
		}
		if len(resp.Value) != 8 {
			t.Errorf("INCREMENT of %q: expected an 8-byte value, got %v", test.value, resp.Value)
		} else if got := binary.BigEndian.Uint64(resp.Value); got != test.result {
			t.Errorf("INCREMENT of %q: expected %d, got %d", test.value, test.result, got)
		}
	}
}

// kv_engine refuses an Add of an existing document first. It refuses another
// sub-document mutation of a locked document only when it carries no CAS, or
// the CAS a GET reports for a locked document. A supplied
// CAS is checked against the lock when the mutation is stored, and a matching
// one stores the mutation and releases the lock.
func TestSubdocMutationOfLockedDocument(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXerror)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
	lockTime := make([]byte, 4)
	binary.BigEndian.PutUint32(lockTime, 30)
	lockCas := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("doc"), Extras: lockTime}).Cas

	valid := subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "2"}
	failing := subdocSpec{Op: memd.SubDocOpReplace, Path: "missing", Value: "2"}
	mutate := func(cas uint64, spec subdocSpec, add bool) *memd.Packet {
		req := &memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Cas:     cas,
			Value:   encodeMutationSpecs(spec),
		}
		if add {
			req.Extras = []byte{byte(memd.SubdocDocFlagAddDoc)}
		}
		return c.Do(req)
	}

	for _, test := range []struct {
		name   string
		cas    uint64
		spec   subdocSpec
		add    bool
		status memd.StatusCode
	}{
		// Refused before the specs run.
		{name: "no CAS", cas: 0, spec: failing, status: memd.StatusLocked},
		{name: "the Add flag", cas: 0, spec: valid, add: true, status: memd.StatusKeyExists},
		{name: "the CAS of a locked read", cas: 0xffffffffffffffff, spec: failing, status: memd.StatusLocked},
		// Checked against the lock only when stored.
		{name: "another CAS and a failing spec", cas: lockCas + 1, spec: failing, status: memd.StatusSubDocBadMulti},
		{name: "another CAS", cas: lockCas + 1, spec: valid, status: memd.StatusLocked},
	} {
		if resp := mutate(test.cas, test.spec, test.add); resp.Status != test.status {
			t.Errorf("mutation with %s: expected status 0x%02x, got 0x%02x", test.name, uint16(test.status), uint16(resp.Status))
		}
	}

	if resp := mutate(lockCas, valid, false); resp.Status != memd.StatusSuccess {
		t.Fatalf("mutation with the locking CAS: expected success, got status 0x%02x", uint16(resp.Status))
	}
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"a":2}` {
		t.Errorf("expected the mutation stored, got %s", resp.Value)
	}
	if resp.Cas == 0xffffffffffffffff {
		t.Errorf("expected the lock released, GET reports the document locked")
	}
}

// kv_engine's arithmetic_validator refuses a counter request with a CAS
// before it reads the document, locked or not.
func TestCounterRefusesCas(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXerror)

	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras, 1)
	binary.BigEndian.PutUint32(extras[16:], 0xffffffff)
	increment := func(key string, cas uint64) memd.StatusCode {
		t.Helper()
		return c.Do(&memd.Packet{Command: memd.CmdIncrement, Vbucket: c.vbID, Key: []byte(key), Cas: cas, Extras: extras}).Status
	}

	textCas := c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("text"), Value: []byte(`"text"`), Extras: make([]byte, 8)}).Cas
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("counter"), Value: []byte("1"), Extras: make([]byte, 8)})
	lockTime := make([]byte, 4)
	binary.BigEndian.PutUint32(lockTime, 30)
	lockCas := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("counter"), Extras: lockTime}).Cas
	for _, test := range []struct {
		name string
		key  string
		cas  uint64
	}{
		{name: "a value that is not a number, with its CAS", key: "text", cas: textCas},
		{name: "a locked counter, with the CAS that locked it", key: "counter", cas: lockCas},
		{name: "a locked counter, with the CAS of a locked read", key: "counter", cas: 0xffffffffffffffff},
	} {
		if status := increment(test.key, test.cas); status != memd.StatusInvalidArgs {
			t.Errorf("INCREMENT of %s: expected status 0x%02x, got 0x%02x", test.name, uint16(memd.StatusInvalidArgs), uint16(status))
		}
	}
}
