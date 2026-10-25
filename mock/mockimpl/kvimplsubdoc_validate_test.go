package mockimpl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

const xattrPath = memd.SubdocFlagXattrPath

// encodeLookupSpecs encodes SUBDOC_MULTI_LOOKUP specs; Value is ignored.
func encodeLookupSpecs(specs ...subdocSpec) []byte {
	var buf []byte
	for _, spec := range specs {
		hdr := make([]byte, 4)
		hdr[0] = uint8(spec.Op)
		hdr[1] = uint8(spec.Flags)
		binary.BigEndian.PutUint16(hdr[2:], uint16(len(spec.Path)))
		buf = append(buf, hdr...)
		buf = append(buf, spec.Path...)
	}
	return buf
}

// The expected statuses follow kv_engine's subdoc_multi_validator and the
// spec traits it checks (daemon/subdocument_validators.cc@v7.0.5,
// daemon/subdocument_traits.h@v7.0.5). The requests name a missing document,
// so a request that passes validation answers key_not_found.
func TestSubdocMultiValidation(t *testing.T) {
	xattrConn := newTestKvConn(t, memd.FeatureXattr)
	plainConn := newTestKvConn(t)
	collectionsConn := newTestKvConn(t, memd.FeatureCollections)

	dictSet := func(path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpDictSet, Flags: flags, Path: path, Value: "1"}
	}
	get := func(path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpGet, Flags: flags, Path: path}
	}
	seventeen := make([]subdocSpec, 17)
	for i := range seventeen {
		seventeen[i] = dictSet("a", 0)
	}

	tests := []struct {
		name        string
		lookup      bool
		noXattr     bool
		collections bool
		key         string
		noKey       bool
		collection  uint32
		datatype    uint8
		extras      []byte
		cas         uint64
		specs       []subdocSpec
		value       []byte
		status      memd.StatusCode
	}{
		{name: "valid", specs: []subdocSpec{dictSet("a", 0)}, status: memd.StatusKeyNotFound},
		{name: "valid lookup", lookup: true, specs: []subdocSpec{get("a", 0)}, status: memd.StatusKeyNotFound},

		{name: "lookup without key", lookup: true, noKey: true, specs: []subdocSpec{get("a", 0)}, status: memd.StatusInvalidArgs},
		{name: "mutation without key", noKey: true, specs: []subdocSpec{dictSet("a", 0)}, status: memd.StatusInvalidArgs},
		{name: "lookup with json datatype", lookup: true, datatype: uint8(memd.DatatypeFlagJSON), specs: []subdocSpec{get("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "lookup with key of 251 bytes", lookup: true, key: strings.Repeat("k", 251), specs: []subdocSpec{get("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "key of only a collection id", collections: true, noKey: true, specs: []subdocSpec{dictSet("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "no logical key", lookup: true, collections: true, noKey: true, collection: 128, specs: []subdocSpec{get("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "reserved collection", lookup: true, collections: true, collection: 1, specs: []subdocSpec{get("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "logical key of 247 bytes", collections: true, collection: 8, key: strings.Repeat("k", 247),
			specs: []subdocSpec{dictSet("a", 0)}, status: memd.StatusInvalidArgs},
		{name: "key of 250 bytes in the default collection", collections: true, key: strings.Repeat("k", 250),
			specs: []subdocSpec{dictSet("a", 0)}, status: memd.StatusKeyNotFound},

		{name: "two xattr keys", specs: []subdocSpec{dictSet("x.a", xattrPath), dictSet("y", xattrPath)},
			status: memd.StatusSubDocXattrInvalidKeyCombo},
		{name: "two xattr keys after a failing spec",
			specs:  []subdocSpec{{Op: memd.SubDocOpCounter, Flags: xattrPath, Path: "x.n", Value: "1"}, dictSet("y", xattrPath)},
			status: memd.StatusSubDocXattrInvalidKeyCombo},
		{name: "two xattr keys lookup", lookup: true, specs: []subdocSpec{get("x", xattrPath), get("y", xattrPath)},
			status: memd.StatusSubDocXattrInvalidKeyCombo},
		{name: "xattr key and index of it", lookup: true, specs: []subdocSpec{get("x", xattrPath), get("x[0]", xattrPath), get("$document", xattrPath)},
			status: memd.StatusKeyNotFound},
		{name: "vattr mutation", specs: []subdocSpec{dictSet("$document.a", xattrPath)}, status: memd.StatusSubDocXattrCannotModifyVAttr},
		{name: "xattr without hello", noXattr: true, specs: []subdocSpec{dictSet("x", xattrPath)}, status: memd.StatusNotSupported},
		{name: "empty xattr key", specs: []subdocSpec{dictSet("", xattrPath)}, status: memd.StatusCode(0x87)},
		{name: "xattr key starting with punctuation", specs: []subdocSpec{dictSet("#x", xattrPath)}, status: memd.StatusCode(0x87)},
		{name: "xattr key of only underscore", specs: []subdocSpec{dictSet("_.a", xattrPath)}, status: memd.StatusCode(0x87)},
		{name: "xattr key of 16 bytes", specs: []subdocSpec{dictSet(strings.Repeat("k", 16), xattrPath)}, status: memd.StatusCode(0x87)},
		{name: "xattr key of 15 bytes", specs: []subdocSpec{dictSet(strings.Repeat("k", 15), xattrPath)}, status: memd.StatusKeyNotFound},
		{name: "xattr key with invalid utf-8", specs: []subdocSpec{dictSet("k\xff", xattrPath)}, status: memd.StatusCode(0x87)},
		{name: "unknown macro", specs: []subdocSpec{{Op: memd.SubDocOpDictSet, Flags: xattrPath | memd.SubdocFlagExpandMacros, Path: "x", Value: `"${foo}"`}},
			status: memd.StatusSubDocXattrUnknownMacro},
		{name: "document macro", specs: []subdocSpec{{Op: memd.SubDocOpDictSet, Flags: xattrPath | memd.SubdocFlagExpandMacros, Path: "x", Value: `"${$document.CAS}"`}},
			status: memd.StatusKeyNotFound},
		{name: "macro without xattr", specs: []subdocSpec{{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagExpandMacros, Path: "a", Value: `"${Mutation.CAS}"`}},
			status: memd.StatusSubDocXattrInvalidFlagCombo},

		{name: "create path on insert", specs: []subdocSpec{{Op: memd.SubDocOpArrayInsert, Flags: mkdirP, Path: "a[0]", Value: "1"}},
			status: memd.StatusInvalidArgs},
		{name: "create path on replace", specs: []subdocSpec{{Op: memd.SubDocOpReplace, Flags: mkdirP, Path: "a", Value: "1"}},
			status: memd.StatusInvalidArgs},
		{name: "create path on delete", specs: []subdocSpec{{Op: memd.SubDocOpDelete, Flags: mkdirP, Path: "a"}},
			status: memd.StatusInvalidArgs},
		{name: "create path on get", lookup: true, specs: []subdocSpec{get("a", mkdirP)}, status: memd.StatusInvalidArgs},
		{name: "macros on counter", specs: []subdocSpec{{Op: memd.SubDocOpCounter, Flags: xattrPath | memd.SubdocFlagExpandMacros, Path: "x", Value: "1"}},
			status: memd.StatusInvalidArgs},
		{name: "macros on add unique", specs: []subdocSpec{{Op: memd.SubDocOpArrayAddUnique, Flags: xattrPath | memd.SubdocFlagExpandMacros, Path: "x", Value: `"${Mutation.CAS}"`}},
			status: memd.StatusInvalidArgs},
		{name: "xattr on whole document", specs: []subdocSpec{{Op: memd.SubDocOpSetDoc, Flags: xattrPath, Value: "{}"}},
			status: memd.StatusInvalidArgs},
		{name: "unused flag", specs: []subdocSpec{dictSet("a", 0x02)}, status: memd.StatusInvalidArgs},
		{name: "invalid flags checked before xattr key", specs: []subdocSpec{dictSet("x", xattrPath), {Op: memd.SubDocOpDelete, Flags: xattrPath | mkdirP, Path: "y"}},
			status: memd.StatusInvalidArgs},

		{name: "mkdoc with delete", extras: []byte{uint8(memd.SubdocDocFlagMkDoc)}, specs: []subdocSpec{{Op: memd.SubDocOpDelete, Path: "a"}},
			status: memd.StatusInvalidArgs},
		{name: "access deleted with set document", extras: []byte{uint8(memd.SubdocDocFlagAccessDeleted)}, specs: []subdocSpec{{Op: memd.SubDocOpSetDoc, Value: "{}"}},
			status: memd.StatusInvalidArgs},
		{name: "mkdoc on lookup", lookup: true, extras: []byte{uint8(memd.SubdocDocFlagMkDoc)}, specs: []subdocSpec{get("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "add with cas", extras: []byte{uint8(memd.SubdocDocFlagAddDoc)}, cas: 1, specs: []subdocSpec{dictSet("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "add and mkdoc", extras: []byte{uint8(memd.SubdocDocFlagAddDoc | memd.SubdocDocFlagMkDoc)}, specs: []subdocSpec{dictSet("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "revive without access deleted", extras: []byte{uint8(memd.SubdocDocFlagReviveDocument)}, specs: []subdocSpec{dictSet("a", 0)},
			status: memd.StatusInvalidArgs},
		{name: "unknown document flag", extras: []byte{0x20}, specs: []subdocSpec{dictSet("a", 0)}, status: memd.StatusInvalidArgs},
		{name: "lookup with expiry", lookup: true, extras: make([]byte, 4), specs: []subdocSpec{get("a", 0)}, status: memd.StatusInvalidArgs},

		{name: "xattr after body", specs: []subdocSpec{dictSet("a", 0), dictSet("x", xattrPath)}, status: memd.StatusCode(0xd4)},
		{name: "xattr after body lookup", lookup: true, specs: []subdocSpec{get("a", 0), get("x", xattrPath)}, status: memd.StatusCode(0xd4)},
		{name: "spec after delete document", specs: []subdocSpec{{Op: memd.SubDocOpDeleteDoc}, dictSet("a", 0)}, status: memd.StatusSubDocBadCombo},
		{name: "delete document after body", specs: []subdocSpec{dictSet("a", 0), {Op: memd.SubDocOpDeleteDoc}}, status: memd.StatusSubDocBadCombo},
		{name: "lookup in mutation", specs: []subdocSpec{{Op: memd.SubDocOpGet, Path: "a"}}, status: memd.StatusSubDocBadCombo},
		{name: "mutation in lookup", lookup: true, specs: []subdocSpec{{Op: memd.SubDocOpDelete, Path: "a"}}, status: memd.StatusSubDocBadCombo},
		{name: "add document", specs: []subdocSpec{{Op: memd.SubDocOpAddDoc, Value: "{}"}}, status: memd.StatusSubDocBadCombo},
		{name: "seventeen specs", specs: seventeen, status: memd.StatusSubDocBadCombo},
		{name: "no specs", value: []byte{}, status: memd.StatusSubDocBadCombo},
		{name: "truncated spec", value: encodeMutationSpecs(dictSet("a", 0))[:9], status: memd.StatusInvalidArgs},

		{name: "empty body path", specs: []subdocSpec{dictSet("", 0)}, status: memd.StatusInvalidArgs},
		{name: "path on whole document", specs: []subdocSpec{{Op: memd.SubDocOpSetDoc, Path: "a", Value: "{}"}}, status: memd.StatusInvalidArgs},
		{name: "path too long", specs: []subdocSpec{dictSet(strings.Repeat("a", 1025), 0)}, status: memd.StatusInvalidArgs},
		{name: "missing value", specs: []subdocSpec{{Op: memd.SubDocOpDictSet, Path: "a"}}, status: memd.StatusInvalidArgs},
		{name: "value on delete", specs: []subdocSpec{{Op: memd.SubDocOpDelete, Path: "a", Value: "1"}}, status: memd.StatusInvalidArgs},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := xattrConn
			if test.noXattr {
				c = plainConn
			}
			if test.collections {
				c = collectionsConn
			}
			key := []byte("missing")
			if test.key != "" {
				key = []byte(test.key)
			}
			if test.noKey {
				key = nil
			}
			command := memd.CmdSubDocMultiMutation
			value := test.value
			if test.lookup {
				command = memd.CmdSubDocMultiLookup
			}
			if value == nil {
				if test.lookup {
					value = encodeLookupSpecs(test.specs...)
				} else {
					value = encodeMutationSpecs(test.specs...)
				}
			}
			resp := c.Do(&memd.Packet{
				Command:      command,
				Vbucket:      c.vbID,
				Key:          key,
				CollectionID: test.collection,
				Datatype:     test.datatype,
				Extras:       test.extras,
				Cas:          test.cas,
				Value:        value,
			})
			if resp.Status != test.status {
				t.Errorf("expected status 0x%02x, got 0x%02x", uint16(test.status), uint16(resp.Status))
			}
		})
	}
}

// A refused request leaves an existing document unchanged even when its
// first spec would succeed.
func TestSubdocMultiValidationBeforeExecution(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"a":1}`),
		Extras:  make([]byte, 8),
	})
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value: encodeMutationSpecs(
			subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x", Value: "1"},
			subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "2"},
			subdocSpec{Op: memd.SubDocOpArrayInsert, Flags: mkdirP, Path: "c[0]", Value: "1"},
		),
	})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	if !bytes.Contains(resp.Value, []byte(`"Request flags invalid"`)) {
		t.Errorf("expected kv_engine's error context, got %s", resp.Value)
	}

	get := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(get.Value) != `{"a":1}` {
		t.Errorf("refused multi-mutation was stored: %s", get.Value)
	}
	if status, _ := lookupXattr(t, c, "doc", "x"); status != memd.StatusSubDocPathNotFound {
		t.Errorf("refused multi-mutation stored xattr x")
	}
}

// subjson's Path::parse (subdoc/path.cc@v7.0.5) accepts an index of digits,
// an empty index meaning 0, and -1, and only '[' or '.' after an index. A key
// ends at a '.', which may end the path. A key keeps its escapes and matches
// a document key with the same text once the document key's \u escapes are
// decoded (jsonsl_path_match, HashKey::get_hk).
func TestSubdocPathSyntax(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{"a":[1,2],"b":{"c":3},"a.b":4,"a` + "`" + `b":5,"a\"b":6,"a/b":7,"c\/d":9,"x":{"y":[{"z":8}]}}`),
		Extras:  make([]byte, 8),
	})

	components := func(n int) string {
		return strings.TrimSuffix(strings.Repeat("q.", n), ".")
	}

	tests := []struct {
		name   string
		path   string
		status memd.StatusCode
		value  string
	}{
		{path: "a[-1]", value: "2"},
		{path: "a[-2]", status: memd.StatusSubDocPathInvalid},
		{path: "a[+1]", status: memd.StatusSubDocPathInvalid},
		{path: "a[]", value: "1"},
		{path: "a[01]", value: "2"},
		{path: "a[1]x", status: memd.StatusSubDocPathInvalid},
		{path: "a[1].", value: "2"},
		{path: "b.", value: `{"c":3}`},
		{path: "b..c", status: memd.StatusSubDocPathInvalid},
		{path: "a[1", status: memd.StatusSubDocPathInvalid},
		{path: "x.y[0].z", value: "8"},
		{path: "`a`[0]", value: "1"},
		{path: "`b`.c", value: "3"},
		{path: "a`.`b", value: "4"},
		{path: "`a.b`", value: "4"},
		{path: "`a``b`", value: "5"},
		{path: `a\"b`, value: "6"},
		{path: `a\/b`, status: memd.StatusSubDocPathNotFound},
		{path: `c\/d`, value: "9"},
		{path: `c/d`, status: memd.StatusSubDocPathNotFound},
		{path: `a\u0062`, status: memd.StatusSubDocPathInvalid},
		{path: `a\x`, status: memd.StatusSubDocPathInvalid},
		{path: `a"b`, status: memd.StatusSubDocPathInvalid},
		{name: "control byte", path: "a\x01b", status: memd.StatusSubDocPathInvalid},
		{path: "a]", status: memd.StatusSubDocPathInvalid},
		{path: "`a", status: memd.StatusSubDocPathInvalid},
		{path: "b.``", status: memd.StatusSubDocPathInvalid},
		{name: "31 components", path: components(31), status: memd.StatusSubDocPathNotFound},
		{name: "32 components", path: components(32), status: memd.StatusSubDocPathTooBig},
		{path: "a[18446744073709551615]", value: "2"},
		{path: "a[30000000000000000000]", status: memd.StatusSubDocPathNotFound},
		{path: "a[9223372036854775808]", status: memd.StatusSubDocPathNotFound},
		{path: "a[18446744073709551616]", status: memd.StatusSubDocPathInvalid},
		{name: "32 components with indexes", path: "a" + strings.Repeat("[0]", 31), status: memd.StatusSubDocPathTooBig},
	}
	for _, test := range tests {
		name := test.name
		if name == "" {
			name = test.path
		}
		t.Run(name, func(t *testing.T) {
			resp := c.Do(&memd.Packet{
				Command: memd.CmdSubDocMultiLookup,
				Vbucket: c.vbID,
				Key:     []byte("doc"),
				Value:   encodeLookupSpecs(subdocSpec{Op: memd.SubDocOpGet, Path: test.path}),
			})
			if len(resp.Value) < 6 {
				t.Fatalf("lookup failed with status 0x%02x", uint16(resp.Status))
			}
			status := memd.StatusCode(binary.BigEndian.Uint16(resp.Value))
			if status != test.status {
				t.Errorf("expected spec status 0x%02x, got 0x%02x", uint16(test.status), uint16(status))
			}
			if test.status == memd.StatusSuccess && string(resp.Value[6:]) != test.value {
				t.Errorf("expected %s, got %s", test.value, resp.Value[6:])
			}
		})
	}
}

// kv_engine resolves a virtual attribute when the spec runs
// (subdoc_operate_one_path in daemon/subdocument.cc@v7.0.5): $document,
// $vbucket and $XTOC, and xattr_unknown_vattr for any other. A $document
// macro naming no field is xattr_unknown_vattr_macro for that spec.
func TestSubdocVirtualAttributes(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{}`),
		Extras:  make([]byte, 8),
	})

	for _, test := range []struct {
		path   string
		status memd.StatusCode
		value  string
	}{
		{path: "$foo", status: memd.StatusSubDocXattrUnknownVAttr},
		{path: "$foo.bar", status: memd.StatusSubDocXattrUnknownVAttr},
	} {
		t.Run(test.path, func(t *testing.T) {
			if status, value := lookupXattr(t, c, "doc", test.path); status != test.status {
				t.Errorf("expected spec status 0x%02x, got 0x%02x", uint16(test.status), uint16(status))
			} else if test.status == memd.StatusSuccess && value != test.value {
				t.Errorf("expected %s, got %s", test.value, value)
			}
		})
	}

	t.Run("unknown document macro", func(t *testing.T) {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Value: encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath | memd.SubdocFlagExpandMacros,
				Path: "x", Value: `"${$document.nope}"`}),
		})
		expectSpecError(t, resp, 0, memd.StatusCode(0xd5))
	})
}

// kv_engine enables xattrs on every bucket type, and on a connection with no
// bucket selected, so validation depends only on the HELLO feature.
func TestSubdocXattrOnAnyBucket(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	bucket, err := c.cluster.AddBucket(mock.NewBucketOptions{Name: "cache", Type: mock.BucketTypeMemcached})
	if err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("cache")})
	vbID := vbucketOwnedAs(t, c, bucket, 0)

	t.Run("memcached bucket", func(t *testing.T) {
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: vbID,
			Key:     []byte("doc"),
			Extras:  []byte{uint8(memd.SubdocDocFlagMkDoc)},
			Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x", Value: "1"}),
		})
		if resp.Status != memd.StatusSuccess {
			t.Errorf("expected success, got status 0x%02x", uint16(resp.Status))
		}
	})

	netConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", c.cluster.Nodes()[0].KvService().ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial kv service: %v", err)
	}
	t.Cleanup(func() { _ = netConn.Close() })
	noBucket := &testKvConn{t: t, cluster: c.cluster, netConn: netConn, conn: memd.NewConn(netConn)}
	hello := make([]byte, 2)
	binary.BigEndian.PutUint16(hello, uint16(memd.FeatureXattr))
	noBucket.mustSucceed(&memd.Packet{Command: memd.CmdHello, Value: hello})
	noBucket.conn.EnableFeature(memd.FeatureXattr)
	noBucket.mustSucceed(&memd.Packet{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("\x00Administrator\x00password")})
	for _, test := range []struct {
		name   string
		spec   subdocSpec
		status memd.StatusCode
	}{
		{name: "valid", spec: subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x", Value: "1"}, status: memd.StatusNoBucket},
		{name: "invalid", spec: subdocSpec{Op: memd.SubDocOpArrayInsert, Flags: mkdirP, Path: "a[0]", Value: "1"}, status: memd.StatusInvalidArgs},
	} {
		t.Run("no bucket "+test.name, func(t *testing.T) {
			resp := noBucket.Do(&memd.Packet{
				Command: memd.CmdSubDocMultiMutation,
				Key:     []byte("doc"),
				Value:   encodeMutationSpecs(test.spec),
			})
			if resp.Status != test.status {
				t.Errorf("expected status 0x%02x, got 0x%02x", uint16(test.status), uint16(resp.Status))
			}
		})
	}
}

// $XTOC lists the xattr keys in the order kv_engine's xattr blob holds them
// (xattr/blob.cc@v7.0.5): a new key is appended, a value of a new size moves
// its key to the end, and a value of the same size keeps its place.
func TestSubdocXtocOrder(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{}`),
		Extras:  make([]byte, 8),
	})

	set := func(key, value string) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: key, Value: value}
	}
	for _, test := range []struct {
		name string
		spec subdocSpec
		xtoc string
	}{
		{name: "add y", spec: set("y", "1"), xtoc: `["y"]`},
		{name: "add x", spec: set("x", "1"), xtoc: `["y","x"]`},
		{name: "same size", spec: set("y", "2"), xtoc: `["y","x"]`},
		{name: "new size", spec: set("y", "10"), xtoc: `["x","y"]`},
		{name: "remove", spec: subdocSpec{Op: memd.SubDocOpDelete, Flags: xattrPath, Path: "x"}, xtoc: `["y"]`},
		{name: "add again", spec: set("x", "1"), xtoc: `["y","x"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			resp := c.Do(&memd.Packet{
				Command: memd.CmdSubDocMultiMutation,
				Vbucket: c.vbID,
				Key:     []byte("doc"),
				Value:   encodeMutationSpecs(test.spec),
			})
			if resp.Status != memd.StatusSuccess {
				t.Fatalf("mutation failed with status 0x%02x", uint16(resp.Status))
			}
			if status, value := lookupXattr(t, c, "doc", "$XTOC"); status != memd.StatusSuccess || value != test.xtoc {
				t.Errorf("expected %s, got status 0x%02x value %s", test.xtoc, uint16(status), value)
			}
		})
	}
}

// kv_engine stores an xattr under the key text of the spec's path and runs
// the spec on {"<key>":<value>} (daemon/subdocument.cc@v7.0.5), so a key with
// JSON escapes resolves to the decoded key in that document, and the value
// keeps '<', '>' and '&' unescaped. Each key is set while missing, then over
// an existing value.
func TestSubdocXattrKeysWithEscapes(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Value:   []byte(`{}`),
		Extras:  make([]byte, 8),
	})

	for _, test := range []struct {
		key   string
		value string
	}{
		{key: `a\/\/b`, value: "5"},
		{key: `a\/b`, value: `"xyz"`},
		{key: "a<b", value: `"<&>"`},
		{key: "a&b", value: `{"c":"a&b"}`},
		{key: `a\bb`, value: "[1]"},
	} {
		t.Run(test.key, func(t *testing.T) {
			for _, value := range []string{test.value, "10", test.value} {
				resp := c.Do(&memd.Packet{
					Command: memd.CmdSubDocMultiMutation,
					Vbucket: c.vbID,
					Key:     []byte("doc"),
					Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: test.key, Value: value}),
				})
				if resp.Status != memd.StatusSuccess {
					t.Fatalf("setting %s failed with status 0x%02x", value, uint16(resp.Status))
				}
				if status, got := lookupXattr(t, c, "doc", test.key); status != memd.StatusSuccess || got != value {
					t.Errorf("expected %s, got status 0x%02x value %s", value, uint16(status), got)
				}
			}
		})
	}
}

// doRawSubdocLookup sends a SUBDOC_MULTI_LOOKUP of path with key sent as
// given, past memd's encoding of the collection ID.
func doRawSubdocLookup(t *testing.T, c *testKvConn, key []byte, path string) *memd.Packet {
	t.Helper()

	value := encodeLookupSpecs(subdocSpec{Op: memd.SubDocOpGet, Path: path})
	c.opaque++
	header := make([]byte, 24)
	header[0] = byte(memd.CmdMagicReq)
	header[1] = byte(memd.CmdSubDocMultiLookup)
	binary.BigEndian.PutUint16(header[2:], uint16(len(key)))
	binary.BigEndian.PutUint16(header[6:], c.vbID)
	binary.BigEndian.PutUint32(header[8:], uint32(len(key)+len(value)))
	binary.BigEndian.PutUint32(header[12:], c.opaque)
	if _, err := c.netConn.Write(append(append(header, key...), value...)); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}
	resp, _, err := c.conn.ReadPacket()
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	return resp
}

// is_document_key_valid (daemon/mcbp_validators.cc@v7.0.5) runs after
// verify_header has required a key, and refuses a collection ID that is not
// encoded in the fewest bytes.
func TestSubdocKeyEncoding(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)

	for _, test := range []struct {
		name    string
		key     []byte
		context string
	}{
		{name: "no key", key: nil, context: "Request must include key"},
		{name: "only a collection id", key: []byte{0x00}, context: "Key length must be >= 2"},
		{name: "non-canonical collection id", key: []byte{0x80, 0x00, 'k'}, context: "Key contains non-canonical leb128"},
		{name: "non-canonical collection id without logical key", key: []byte{0x88, 0x00}, context: "Key contains non-canonical leb128"},
	} {
		t.Run(test.name, func(t *testing.T) {
			resp := doRawSubdocLookup(t, c, test.key, "a")
			if resp.Status != memd.StatusInvalidArgs {
				t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
			}
			if !bytes.Contains(resp.Value, []byte(`"`+test.context+`"`)) {
				t.Errorf("expected error context %q, got %s", test.context, resp.Value)
			}
		})
	}
}
