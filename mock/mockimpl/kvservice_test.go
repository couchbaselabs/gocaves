package mockimpl

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/golang/snappy"
)

func TestCompressedValueIsInflated(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSnappy)

	doc := `{"counter":1,"padding":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	c.mustSucceed(&memd.Packet{
		Command:  memd.CmdSet,
		Vbucket:  c.vbID,
		Datatype: uint8(memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed),
		Key:      []byte("compressed"),
		Value:    snappy.Encode(nil, []byte(doc)),
		Extras:   make([]byte, 8),
	})

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("compressed")})
	if resp.Datatype&uint8(memd.DatatypeFlagCompressed) != 0 || string(resp.Value) != doc {
		t.Errorf("expected inflated %s, got datatype 0x%02x value %q", doc, resp.Datatype, resp.Value)
	}

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("compressed"),
		Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpCounter, Path: "counter", Value: "1"}),
	})
}

func TestHooksSeeCompressedRequest(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSnappy)

	// Hooks such as KvExpect.Wait keep the packet after next returns.
	seen := make(chan memd.Packet, 2)
	c.cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command == memd.CmdSet {
			seen <- *pak
		}
		next()
		if pak.Command == memd.CmdSet {
			seen <- *pak
		}
	})

	doc := `{"padding":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	compressed := snappy.Encode(nil, []byte(doc))
	c.mustSucceed(&memd.Packet{
		Command:  memd.CmdSet,
		Vbucket:  c.vbID,
		Datatype: uint8(memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed),
		Key:      []byte("compressed"),
		Value:    compressed,
		Extras:   make([]byte, 8),
	})

	for _, when := range []string{"before next", "after next"} {
		set := <-seen
		if set.Datatype&uint8(memd.DatatypeFlagCompressed) == 0 || !bytes.Equal(set.Value, compressed) {
			t.Errorf("%s, hook saw datatype 0x%02x value %q, not the compressed request", when, set.Datatype, set.Value)
		}
	}

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("compressed")})
	if string(resp.Value) != doc {
		t.Errorf("expected stored %s, got %q", doc, resp.Value)
	}
}

func TestCompressedValueOverItemLimitIsTooBig(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSnappy)

	// Inflates past the 20 MiB item limit, within the 30 MiB inflate limit.
	resp := c.Do(&memd.Packet{
		Command:  memd.CmdSet,
		Vbucket:  c.vbID,
		Datatype: uint8(memd.DatatypeFlagCompressed),
		Key:      []byte("compressed"),
		Value:    snappy.Encode(nil, make([]byte, 25*1024*1024)),
		Extras:   make([]byte, 8),
	})
	if resp.Status != memd.StatusTooBig {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusTooBig), uint16(resp.Status))
	}
}

func TestCompressedValueRequiresSnappy(t *testing.T) {
	c := newTestKvConn(t)

	resp := c.Do(&memd.Packet{
		Command:  memd.CmdSet,
		Vbucket:  c.vbID,
		Datatype: uint8(memd.DatatypeFlagCompressed),
		Key:      []byte("compressed"),
		Value:    snappy.Encode(nil, []byte(`{}`)),
		Extras:   make([]byte, 8),
	})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
}

func TestCompressedValueTooLargeToInflate(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureSnappy)

	// A snappy block opens with its inflated length as a varint. This one
	// claims 0xffffffff bytes and carries nothing.
	resp := c.Do(&memd.Packet{
		Command:  memd.CmdSet,
		Vbucket:  c.vbID,
		Datatype: uint8(memd.DatatypeFlagCompressed),
		Key:      []byte("compressed"),
		Value:    []byte{0xff, 0xff, 0xff, 0xff, 0x0f},
		Extras:   make([]byte, 8),
	})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
}

func TestInflateRefusesOversizedHeaderBeforeAllocating(t *testing.T) {
	cli := &kvClient{features: []memd.HelloFeature{memd.FeatureSnappy}}

	// A snappy block opens with its inflated length as a varint. This one
	// claims 64 MiB and carries nothing.
	value := make([]byte, binary.MaxVarintLen64)
	value = value[:binary.PutUvarint(value, 64*1024*1024)]

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, ok := inflateRequestValue(cli, value)
	runtime.ReadMemStats(&after)

	if ok {
		t.Fatalf("inflated a value whose header exceeds the limit")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= maxInflatedSize {
		t.Errorf("allocated %d bytes for a value that was refused", allocated)
	}
}
