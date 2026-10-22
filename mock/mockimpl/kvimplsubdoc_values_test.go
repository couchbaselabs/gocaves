package mockimpl

import (
	"encoding/binary"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func encodeLookupSpec(op memd.SubDocOpType, path string) []byte {
	buf := make([]byte, 4, 4+len(path))
	buf[0] = uint8(op)
	binary.BigEndian.PutUint16(buf[2:], uint16(len(path)))
	return append(buf, path...)
}

// expectSpecError checks a failed SUBDOC_MULTI_MUTATION response: the index
// of the failed spec, then its status.
func expectSpecError(t *testing.T, resp *memd.Packet, index uint8, status memd.StatusCode) {
	t.Helper()

	if resp.Status != memd.StatusSubDocBadMulti {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSubDocBadMulti), uint16(resp.Status))
	}
	if len(resp.Value) != 3 {
		t.Fatalf("expected a 3-byte body, got %v", resp.Value)
	}
	if resp.Value[0] != index {
		t.Errorf("expected failed spec index %d, got %d", index, resp.Value[0])
	}
	if got := memd.StatusCode(binary.BigEndian.Uint16(resp.Value[1:])); got != status {
		t.Errorf("expected spec status 0x%02x, got 0x%02x", uint16(status), uint16(got))
	}
}

func TestSubdocOnNonJSONDocument(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("raw"),
		Value:   []byte("string"),
		Extras:  make([]byte, 8),
	})

	for _, op := range []memd.SubDocOpType{memd.SubDocOpGet, memd.SubDocOpExists} {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiLookup,
			Vbucket: c.vbID,
			Key:     []byte("raw"),
			Value:   encodeLookupSpec(op, "non-exist"),
		})
		if resp.Status != memd.StatusSubDocBadMulti {
			t.Fatalf("op 0x%02x: expected status 0x%02x, got 0x%02x", uint8(op), uint16(memd.StatusSubDocBadMulti), uint16(resp.Status))
		}
		if len(resp.Value) != 6 {
			t.Fatalf("op 0x%02x: expected a 6-byte body, got %v", uint8(op), resp.Value)
		}
		if got := memd.StatusCode(binary.BigEndian.Uint16(resp.Value)); got != memd.StatusSubDocNotJSON {
			t.Errorf("op 0x%02x: expected spec status 0x%02x, got 0x%02x", uint8(op), uint16(memd.StatusSubDocNotJSON), uint16(got))
		}
	}

	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("raw"),
		Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "dict", Value: "1"}),
	})
	expectSpecError(t, resp, 0, memd.StatusSubDocNotJSON)
}

func TestSubdocMutationWithNonJSONValue(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"array":[]}`),
		Extras:  make([]byte, 8),
	})

	for _, spec := range []subdocSpec{
		{Op: memd.SubDocOpDictSet, Path: "dict", Value: "non-json"},
		{Op: memd.SubDocOpArrayPushLast, Path: "array", Value: "non-json"},
		{Op: memd.SubDocOpArrayAddUnique, Path: "array", Value: "non-json"},
	} {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Value:   encodeMutationSpecs(spec),
		})
		expectSpecError(t, resp, 0, memd.StatusSubDocCantInsert)
	}
}

// counterResult returns the value of a successful single-spec counter
// mutation.
func counterResult(t *testing.T, resp *memd.Packet) string {
	t.Helper()

	if resp.Status != memd.StatusSuccess {
		t.Fatalf("counter failed with status 0x%02x, body %v", uint16(resp.Status), resp.Value)
	}
	if len(resp.Value) < 7 {
		t.Fatalf("expected a result entry, got %v", resp.Value)
	}
	return string(resp.Value[7:])
}

func TestSubdocCounterStaysWithinInt64(t *testing.T) {
	c := newTestKvConn(t)

	counter := func(doc, delta string) *memd.Packet {
		c.mustSucceed(&memd.Packet{
			Command: memd.CmdSet,
			Vbucket: c.vbID,
			Key:     []byte("counter"),
			Value:   []byte(doc),
			Extras:  make([]byte, 8),
		})
		return c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("counter"),
			Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpCounter, Path: "counter", Value: delta}),
		})
	}

	if got := counterResult(t, counter(`{"counter":0}`, "9223372036854775807")); got != "9223372036854775807" {
		t.Errorf("expected 9223372036854775807, got %s", got)
	}
	expectSpecError(t, counter(`{"counter":9223372036854775807}`, "1"), 0, memd.StatusSubDocCantInsert)

	if got := counterResult(t, counter(`{"counter":-9223372036854775807}`, "-1")); got != "-9223372036854775808" {
		t.Errorf("expected -9223372036854775808, got %s", got)
	}
	expectSpecError(t, counter(`{"counter":-9223372036854775808}`, "-1"), 0, memd.StatusSubDocCantInsert)
}

func TestSubdocMutationKeepsLargeIntegers(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"id":9007199254740993}`),
		Extras:  make([]byte, 8),
	})
	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Path: "x", Value: "1"}),
	})

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"id":9007199254740993,"x":1}` {
		t.Errorf("unexpected document %s", resp.Value)
	}
}

// subjson compares an array-add-unique value with the array's elements by
// their JSON text, so numbers that are equal in value but written
// differently are distinct.
func TestSubdocArrayAddUniqueComparesText(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"array":[0,9007199254740993]}`),
		Extras:  make([]byte, 8),
	})

	addUnique := func(value string) *memd.Packet {
		return c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpArrayAddUnique, Path: "array", Value: value}),
		})
	}

	expectSpecError(t, addUnique("0"), 0, memd.StatusSubDocPathExists)
	expectSpecError(t, addUnique("9007199254740993"), 0, memd.StatusSubDocPathExists)
	for _, value := range []string{"-0", "9007199254740992"} {
		if resp := addUnique(value); resp.Status != memd.StatusSuccess {
			t.Errorf("adding %s failed with status 0x%02x", value, uint16(resp.Status))
		}
	}

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"array":[0,9007199254740993,-0,9007199254740992]}` {
		t.Errorf("unexpected document %s", resp.Value)
	}
}
