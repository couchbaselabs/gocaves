package mockimpl

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

const mkdirP = memd.SubdocFlagMkDirP

// missingPathTest is one SUBDOC_MULTI_MUTATION spec on a missing path, sent
// after doc is stored and seed is applied. A zero status expects success,
// and any other the spec's error, which leaves the document unchanged.
type missingPathTest struct {
	name   string
	doc    string
	seed   []subdocSpec
	spec   subdocSpec
	status memd.StatusCode
	result string
	want   string
}

func runMissingPathTest(t *testing.T, c *testKvConn, test missingPathTest, stored func(t *testing.T, key string) string) {
	t.Helper()

	key := []byte(t.Name())
	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     key,
		Value:   []byte(test.doc),
		Extras:  make([]byte, 8),
	})
	if len(test.seed) > 0 {
		c.mustSucceed(&memd.Packet{
			Command: memd.CmdSubDocMultiMutation,
			Vbucket: c.vbID,
			Key:     key,
			Value:   encodeMutationSpecs(test.seed...),
		})
	}
	before := stored(t, string(key))
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     key,
		Value:   encodeMutationSpecs(test.spec),
	})

	if test.status != 0 {
		expectSpecError(t, resp, 0, test.status)
		if got := stored(t, string(key)); got != before {
			t.Errorf("failed mutation changed %s to %s", before, got)
		}
		return
	}
	if resp.Status != memd.StatusSuccess {
		t.Fatalf("expected success, got status 0x%02x body %v", uint16(resp.Status), resp.Value)
	}
	if test.result != "" {
		if got := counterResult(t, resp); got != test.result {
			t.Errorf("expected result %s, got %s", test.result, got)
		}
	}
	if got := stored(t, string(key)); got != test.want {
		t.Errorf("expected %s, got %s", test.want, got)
	}
}

// lookupXattr returns the status and value of a SUBDOC_MULTI_LOOKUP of xattr
// path on key.
func lookupXattr(t *testing.T, c *testKvConn, key, path string) (memd.StatusCode, string) {
	t.Helper()

	lookup := encodeLookupSpec(memd.SubDocOpGet, path)
	lookup[1] = uint8(memd.SubdocFlagXattrPath)
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiLookup,
		Vbucket: c.vbID,
		Key:     []byte(key),
		Value:   lookup,
	})
	if len(resp.Value) < 6 {
		t.Fatalf("xattr lookup failed with status 0x%02x", uint16(resp.Status))
	}
	return memd.StatusCode(binary.BigEndian.Uint16(resp.Value)), string(resp.Value[6:])
}

// The expected results follow subjson subdoc/operations.cc@v7.0.5:
// do_arith_op, do_list_append, do_list_prepend, do_mkdir_p and do_insert.
func TestSubdocMutationOnMissingBodyPath(t *testing.T) {
	c := newTestKvConn(t)

	counter := func(path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpCounter, Flags: flags, Path: path, Value: "5"}
	}
	array := func(op memd.SubDocOpType, path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: op, Flags: flags, Path: path, Value: "1"}
	}
	insert := func(path string) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpArrayInsert, Path: path, Value: "2"}
	}

	tests := []missingPathTest{
		{name: "counter top-level leaf", doc: `{}`, spec: counter("n", 0), result: "5", want: `{"n":5}`},
		{name: "counter top-level leaf create path", doc: `{}`, spec: counter("n", mkdirP), result: "5", want: `{"n":5}`},
		{name: "counter leaf in object", doc: `{"o":{"m":1}}`, spec: counter("o.n", 0), result: "5", want: `{"o":{"m":1,"n":5}}`},
		{name: "counter leaf in a[0]", doc: `{"a":[{}]}`, spec: counter("a[0].n", 0), result: "5", want: `{"a":[{"n":5}]}`},
		{name: "counter leaf in a[-1]", doc: `{"a":[{}]}`, spec: counter("a[-1].n", 0), result: "5", want: `{"a":[{"n":5}]}`},
		{name: "counter negative delta", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpCounter, Path: "n", Value: "-5"}, result: "-5", want: `{"n":-5}`},
		{name: "counter missing parent", doc: `{}`, spec: counter("p.n", 0), status: memd.StatusSubDocPathNotFound},
		{name: "counter missing parents create path", doc: `{}`, spec: counter("p.q.n", mkdirP), result: "5", want: `{"p":{"q":{"n":5}}}`},
		{name: "counter a[1] create path", doc: `{"a":[1]}`, spec: counter("a[1]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "counter under [-1] of empty array create path", doc: `{"a":[]}`, spec: counter("a[-1].n", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "counter under [-5] create path", doc: `{"a":[1]}`, spec: counter("a[-5].n", mkdirP), status: memd.StatusSubDocPathInvalid},
		{name: "counter index under missing key", doc: `{}`, spec: counter("p[0]", 0), status: memd.StatusSubDocPathNotFound},
		{name: "counter index under missing key create path", doc: `{}`, spec: counter("p.q[0]", mkdirP), status: memd.StatusSubDocPathInvalid},
		{name: "counter index under missing key in a[0] create path", doc: `{"a":[{}]}`, spec: counter("a[0].q[0]", mkdirP), status: memd.StatusSubDocPathInvalid},
		{name: "counter index under missing a[1] create path", doc: `{"a":[1]}`, spec: counter("a[1][0]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "counter index under number create path", doc: `{"s":1}`, spec: counter("s[0]", mkdirP), status: memd.StatusSubDocPathMismatch},
		{name: "counter path checked before delta", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpCounter, Path: "a[-2]", Value: "0"}, status: memd.StatusSubDocPathInvalid},

		{name: "dict set at index", doc: `{"a":[1]}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: "a[0]", Value: "2"}, status: memd.StatusSubDocPathInvalid},
		{name: "dict add at index under missing key create path", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictAdd, Flags: mkdirP, Path: "p[0]", Value: "2"}, status: memd.StatusSubDocPathInvalid},
		{name: "dict set at index of number", doc: `{"s":1}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: "s[0]", Value: "2"}, status: memd.StatusSubDocPathMismatch},
		{name: "replace at index", doc: `{"a":[1]}`, spec: subdocSpec{Op: memd.SubDocOpReplace, Path: "a[0]", Value: "2"}, want: `{"a":[2]}`},
		{name: "dict set value checked before path", doc: `{"s":1}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: "s.n", Value: "non-json"}, status: memd.StatusSubDocCantInsert},
		{name: "dict set path checked before value", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: "a..b", Value: "non-json"}, status: memd.StatusSubDocPathInvalid},
		{name: "counter under a[0] of missing key create path", doc: `{}`, spec: counter("a[0].b", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "dict set under a[0] of missing key create path", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Flags: mkdirP, Path: "a[0].b", Value: "1"}, status: memd.StatusSubDocPathNotFound},
		{name: "add unique a[0] of missing key create path", doc: `{}`, spec: array(memd.SubDocOpArrayAddUnique, "a[0]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "push last a[0] of missing key create path", doc: `{}`, spec: array(memd.SubDocOpArrayPushLast, "a[0]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "push first at 31 components", doc: `{}`, spec: array(memd.SubDocOpArrayPushFirst, strings.TrimSuffix(strings.Repeat("q.", 31), "."), 0), status: memd.StatusSubDocValueTooDeep},
		{name: "push last at 31 components", doc: `{}`, spec: array(memd.SubDocOpArrayPushLast, strings.TrimSuffix(strings.Repeat("q.", 31), "."), 0), status: memd.StatusSubDocValueTooDeep},
		{name: "dict set at 32 components", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: strings.TrimSuffix(strings.Repeat("q.", 32), "."), Value: "1"}, status: memd.StatusSubDocPathTooBig},
		{name: "counter key in array", doc: `{"a":[]}`, spec: counter("a.n", 0), status: memd.StatusSubDocPathMismatch},

		{name: "add unique missing array", doc: `{}`, spec: array(memd.SubDocOpArrayAddUnique, "l", 0), status: memd.StatusSubDocPathNotFound},
		{name: "add unique missing array create path", doc: `{}`, spec: array(memd.SubDocOpArrayAddUnique, "l", mkdirP), want: `{"l":[1]}`},
		{name: "add unique missing parent", doc: `{}`, spec: array(memd.SubDocOpArrayAddUnique, "p.l", 0), status: memd.StatusSubDocPathNotFound},
		{name: "add unique missing parent create path", doc: `{}`, spec: array(memd.SubDocOpArrayAddUnique, "p.l", mkdirP), want: `{"p":{"l":[1]}}`},
		{name: "add unique [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayAddUnique, "a[-1]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "add unique object", doc: `{"l":[]}`, spec: subdocSpec{Op: memd.SubDocOpArrayAddUnique, Path: "l", Value: `{"a":1}`}, status: memd.StatusSubDocCantInsert},
		{name: "add unique array", doc: `{"l":[]}`, spec: subdocSpec{Op: memd.SubDocOpArrayAddUnique, Path: "l", Value: `[1]`}, status: memd.StatusSubDocCantInsert},
		{name: "add unique value checked before path", doc: `{"s":1}`, spec: subdocSpec{Op: memd.SubDocOpArrayAddUnique, Path: "s.l", Value: `{}`}, status: memd.StatusSubDocCantInsert},
		{name: "add unique under [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayAddUnique, "a[-1].l", mkdirP), status: memd.StatusSubDocPathNotFound},

		{name: "push last missing array", doc: `{}`, spec: array(memd.SubDocOpArrayPushLast, "l", 0), status: memd.StatusSubDocPathNotFound},
		{name: "push last missing parent create path", doc: `{}`, spec: array(memd.SubDocOpArrayPushLast, "p.l", mkdirP), want: `{"p":{"l":[1]}}`},
		{name: "push last [-1] of empty array", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushLast, "a[-1]", 0), status: memd.StatusSubDocPathNotFound},
		{name: "push last [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushLast, "a[-1]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "push last under [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushLast, "a[-1].l", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "push last value checked before path", doc: `{"s":1}`, spec: subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "s.l", Value: "non-json"}, status: memd.StatusSubDocCantInsert},
		{name: "push last at -2", doc: `{"a":[[]]}`, spec: array(memd.SubDocOpArrayPushLast, "a[-2]", 0), status: memd.StatusSubDocPathInvalid},
		{name: "push first [-1] of empty array", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushFirst, "a[-1]", 0), status: memd.StatusSubDocPathNotFound},
		{name: "push first [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushFirst, "a[-1]", mkdirP), status: memd.StatusSubDocPathNotFound},
		{name: "push first under [-1] of empty array create path", doc: `{"a":[]}`, spec: array(memd.SubDocOpArrayPushFirst, "a[-1].l", mkdirP), status: memd.StatusSubDocPathNotFound},

		{name: "insert into empty array", doc: `{"l":[]}`, spec: insert("l[0]"), want: `{"l":[2]}`},
		{name: "insert one past the end", doc: `{"l":[1]}`, spec: insert("l[1]"), want: `{"l":[1,2]}`},
		{name: "insert at the start", doc: `{"l":[1]}`, spec: insert("l[0]"), want: `{"l":[2,1]}`},
		{name: "insert two past the end", doc: `{"l":[1]}`, spec: insert("l[2]"), status: memd.StatusSubDocPathNotFound},
		{name: "insert into missing array", doc: `{}`, spec: insert("l[0]"), status: memd.StatusSubDocPathNotFound},
		{name: "insert at -1", doc: `{"l":[1]}`, spec: insert("l[-1]"), status: memd.StatusSubDocPathInvalid},
		{name: "insert at -1 of empty array", doc: `{"l":[]}`, spec: insert("l[-1]"), status: memd.StatusSubDocPathInvalid},
		{name: "insert at -2", doc: `{"l":[1]}`, spec: insert("l[-2]"), status: memd.StatusSubDocPathInvalid},
		{name: "insert value checked before index", doc: `{"l":[1]}`, spec: subdocSpec{Op: memd.SubDocOpArrayInsert, Path: "l[-1]", Value: "non-json"}, status: memd.StatusSubDocCantInsert},
		{name: "delete at -2", doc: `{"l":[1,2]}`, spec: subdocSpec{Op: memd.SubDocOpDelete, Path: "l[-2]"}, status: memd.StatusSubDocPathInvalid},
		{name: "delete at -1", doc: `{"l":[1,2]}`, spec: subdocSpec{Op: memd.SubDocOpDelete, Path: "l[-1]"}, want: `{"l":[1]}`},
	}

	stored := func(t *testing.T, key string) string {
		return string(c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte(key)}).Value)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runMissingPathTest(t, c, test, stored)
		})
	}
}

// kv_engine runs xattr specs against {} when the document has no xattr of
// that key (daemon/subdocument.cc@v7.0.5), so the first path component is a
// key of an existing object.
func TestSubdocMutationOnMissingXattrPath(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	xattr := func(op memd.SubDocOpType, path string, flags memd.SubdocFlag, value string) subdocSpec {
		return subdocSpec{Op: op, Flags: flags | memd.SubdocFlagXattrPath, Path: path, Value: value}
	}

	seed := func(value string) []subdocSpec {
		return []subdocSpec{xattr(memd.SubDocOpDictSet, "x", 0, value)}
	}

	tests := []missingPathTest{
		{name: "counter key", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x", 0, "5"), result: "5", want: `5`},
		{name: "counter key create path", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x", mkdirP, "5"), result: "5", want: `5`},
		{name: "counter missing key", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x.n", 0, "5"), status: memd.StatusSubDocPathNotFound},
		{name: "counter missing key create path", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x.n", mkdirP, "5"), result: "5", want: `{"n":5}`},
		{name: "counter missing parents create path", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x.p.n", mkdirP, "5"), result: "5", want: `{"p":{"n":5}}`},
		{name: "counter leaf in existing xattr", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x.n", 0, "5"), result: "5", want: `{"m":1,"n":5}`, seed: seed(`{"m":1}`)},
		{name: "counter [-1] of empty array create path", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x[-1]", mkdirP, "5"), status: memd.StatusSubDocPathNotFound, seed: seed(`[]`)},
		{name: "add unique key", doc: `{}`, spec: xattr(memd.SubDocOpArrayAddUnique, "x", 0, "1"), status: memd.StatusSubDocPathNotFound},
		{name: "add unique key create path", doc: `{}`, spec: xattr(memd.SubDocOpArrayAddUnique, "x", mkdirP, "1"), want: `[1]`},
		{name: "add unique missing key create path", doc: `{}`, spec: xattr(memd.SubDocOpArrayAddUnique, "x.l", mkdirP, "1"), want: `{"l":[1]}`},
		{name: "add unique [-1] of empty array create path", doc: `{}`, spec: xattr(memd.SubDocOpArrayAddUnique, "x[-1]", mkdirP, "1"), status: memd.StatusSubDocPathNotFound, seed: seed(`[]`)},
		{name: "push last missing key create path", doc: `{}`, spec: xattr(memd.SubDocOpArrayPushLast, "x.l", mkdirP, "1"), want: `{"l":[1]}`},
		{name: "push last [-1] of empty array", doc: `{}`, spec: xattr(memd.SubDocOpArrayPushLast, "x[-1]", 0, "1"), status: memd.StatusSubDocPathNotFound, seed: seed(`[]`)},
		{name: "push first [-1] of empty array create path", doc: `{}`, spec: xattr(memd.SubDocOpArrayPushFirst, "x[-1]", mkdirP, "1"), status: memd.StatusSubDocPathNotFound, seed: seed(`[]`)},
		{name: "insert into empty array", doc: `{}`, spec: xattr(memd.SubDocOpArrayInsert, "x[0]", 0, "2"), want: `[2]`, seed: seed(`[]`)},
		{name: "insert one past the end", doc: `{}`, spec: xattr(memd.SubDocOpArrayInsert, "x[1]", 0, "2"), want: `[1,2]`, seed: seed(`[1]`)},
		{name: "insert at -1", doc: `{}`, spec: xattr(memd.SubDocOpArrayInsert, "x[-1]", 0, "2"), status: memd.StatusSubDocPathInvalid, seed: seed(`[1]`)},
		{name: "dict set at -2", doc: `{}`, spec: xattr(memd.SubDocOpDictSet, "x[-2]", 0, "2"), status: memd.StatusSubDocPathInvalid, seed: seed(`[1,2]`)},
		{name: "counter index under missing key create path", doc: `{}`, spec: xattr(memd.SubDocOpCounter, "x.p[0]", mkdirP, "5"), status: memd.StatusSubDocPathInvalid},
	}

	stored := func(t *testing.T, key string) string {
		status, value := lookupXattr(t, c, key, "x")
		if status != memd.StatusSuccess {
			return "missing"
		}
		return value
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runMissingPathTest(t, c, test, stored)
		})
	}
}

// A spec that creates a path is not stored when a later spec fails.
func TestSubdocMutationOnMissingPathIsAtomic(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	tests := []struct {
		name  string
		specs []subdocSpec
	}{
		{name: "counter", specs: []subdocSpec{
			{Op: memd.SubDocOpCounter, Path: "n", Value: "1"},
			{Op: memd.SubDocOpReplace, Path: "missing", Value: "1"},
		}},
		{name: "add unique", specs: []subdocSpec{
			{Op: memd.SubDocOpArrayAddUnique, Flags: mkdirP, Path: "l", Value: "1"},
			{Op: memd.SubDocOpReplace, Path: "missing", Value: "1"},
		}},
		{name: "insert", specs: []subdocSpec{
			{Op: memd.SubDocOpArrayInsert, Path: "e[0]", Value: "1"},
			{Op: memd.SubDocOpReplace, Path: "missing", Value: "1"},
		}},
		{name: "xattr counter", specs: []subdocSpec{
			{Op: memd.SubDocOpCounter, Flags: memd.SubdocFlagXattrPath | mkdirP, Path: "x.n", Value: "1"},
			{Op: memd.SubDocOpReplace, Flags: memd.SubdocFlagXattrPath, Path: "x.missing", Value: "1"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := []byte(t.Name())
			c.mustSucceed(&memd.Packet{
				Command: memd.CmdSet,
				Vbucket: c.vbID,
				Key:     key,
				Value:   []byte(`{"e":[]}`),
				Extras:  make([]byte, 8),
			})
			resp := c.Do(&memd.Packet{
				Command: memd.CmdSubDocMultiMutation,
				Vbucket: c.vbID,
				Key:     key,
				Value:   encodeMutationSpecs(test.specs...),
			})
			expectSpecError(t, resp, 1, memd.StatusSubDocPathNotFound)

			get := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: key})
			if string(get.Value) != `{"e":[]}` {
				t.Errorf("failed multi-mutation was stored: %s", get.Value)
			}
			if status, value := lookupXattr(t, c, string(key), "x"); status != memd.StatusSubDocPathNotFound {
				t.Errorf("failed multi-mutation stored xattr x: %s", value)
			}
		})
	}
}

// kv_engine rejects specs naming two xattr keys while validating the request
// (daemon/subdocument_validators.cc@v7.0.5), before the counter runs, and
// creates no document.
func TestSubdocCounterWithTwoXattrKeys(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Extras:  []byte{uint8(memd.SubdocDocFlagMkDoc)},
		Value: encodeMutationSpecs(
			subdocSpec{Op: memd.SubDocOpCounter, Flags: memd.SubdocFlagXattrPath | mkdirP, Path: "count", Value: "1"},
			subdocSpec{Op: memd.SubDocOpDictAdd, Flags: memd.SubdocFlagXattrPath | mkdirP, Path: "logs", Value: `"bar1"`},
		),
	})
	if resp.Status != memd.StatusSubDocXattrInvalidKeyCombo {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusSubDocXattrInvalidKeyCombo), uint16(resp.Status))
	}

	resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if resp.Status != memd.StatusKeyNotFound {
		t.Errorf("expected the document to be missing, got status 0x%02x", uint16(resp.Status))
	}
}
