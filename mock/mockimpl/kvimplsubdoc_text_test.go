package mockimpl

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/golang/snappy"
)

// subjson edits the text of a document in place (subdoc/operations.cc@v7.0.5):
// the rest of the document keeps its order and whitespace, a value is
// written as the client sent it, and a new key as written in the path.
func TestSubdocKeepsDocumentText(t *testing.T) {
	c := newTestKvConn(t)

	const doc = `{ "a" : 1 , "b" : [ 1 , 2 ] }`
	tests := []struct {
		name string
		doc  string
		spec subdocSpec
		want string
	}{
		{name: "dict set", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: `{"n": 2}`}, want: `{ "a" : {"n": 2} , "b" : [ 1 , 2 ] }`},
		{name: "dict add", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDictAdd, Path: "c", Value: ` 3 `}, want: `{ "a" : 1 , "b" : [ 1 , 2 ] ,"c": 3 }`},
		{name: "dict add escaped key", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictAdd, Path: `a\"b`, Value: `1`}, want: `{"a\"b":1}`},
		{name: "push last", doc: doc, spec: subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "b", Value: `3, 4`}, want: `{ "a" : 1 , "b" : [ 1 , 2 ,3, 4] }`},
		{name: "push first", doc: doc, spec: subdocSpec{Op: memd.SubDocOpArrayPushFirst, Path: "b", Value: `0`}, want: `{ "a" : 1 , "b" : [ 0,1 , 2 ] }`},
		{name: "push first into empty array", doc: `{ "b" : [ ] }`, spec: subdocSpec{Op: memd.SubDocOpArrayPushFirst, Path: "b", Value: `0`}, want: `{ "b" : [0] }`},
		{name: "push last to root", doc: `[ 1 , 2 ]  `, spec: subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "", Value: `3`}, want: `[ 1 , 2,3]`},
		{name: "insert", doc: doc, spec: subdocSpec{Op: memd.SubDocOpArrayInsert, Path: "b[1]", Value: `9`}, want: `{ "a" : 1 , "b" : [ 1 , 9,2 ] }`},
		{name: "delete first", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDelete, Path: "a"}, want: `{  "b" : [ 1 , 2 ] }`},
		{name: "delete last", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDelete, Path: "b"}, want: `{ "a" : 1  }`},
		{name: "counter", doc: doc, spec: subdocSpec{Op: memd.SubDocOpCounter, Path: "a", Value: `5`}, want: `{ "a" : 6 , "b" : [ 1 , 2 ] }`},
		{name: "counter create path", doc: doc, spec: subdocSpec{Op: memd.SubDocOpCounter, Flags: mkdirP, Path: "x.y", Value: `5`}, want: `{ "a" : 1 , "b" : [ 1 , 2 ] ,"x":{"y":5}}`},
		{name: "push last numbers without a comma", doc: `{"l":[0]}`, spec: subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "l", Value: `1 2`}, want: `{"l":[0,1 2]}`},
		{name: "add unique compares the value as sent", doc: `{"l":["a"]}`, spec: subdocSpec{Op: memd.SubDocOpArrayAddUnique, Path: "l", Value: ` "a" `}, want: `{"l":["a", "a" ]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := []byte(t.Name())
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: key, Value: []byte(test.doc), Extras: make([]byte, 8)})
			resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: key, Value: encodeMutationSpecs(test.spec)})
			got := string(c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: key}).Value)
			if resp.Status != memd.StatusSuccess {
				t.Fatalf("mutation failed with status 0x%02x", uint16(resp.Status))
			}
			if got != test.want {
				t.Errorf("expected %s, got %s", test.want, got)
			}
		})
	}

	t.Run("get", func(t *testing.T) {
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a": [ 1 , {"x" : 2 } ] }`), Extras: make([]byte, 8)})
		resp := c.Do(&memd.Packet{
			Command: memd.CmdSubDocMultiLookup,
			Vbucket: c.vbID,
			Key:     []byte("doc"),
			Value:   encodeLookupSpecs(subdocSpec{Op: memd.SubDocOpGet, Path: "a"}),
		})
		if len(resp.Value) < 6 || string(resp.Value[6:]) != `[ 1 , {"x" : 2 } ]` {
			t.Errorf("unexpected lookup %q", resp.Value)
		}
	})
}

// kv_engine runs every xattr spec of a request on one {"<key>":<value>}
// document and stores the result once (do_xattr_phase in
// daemon/subdocument.cc@v7.0.5), so the xattr blob compares the value it held
// before the request with the one after it.
func TestSubdocXattrStoredOncePerRequest(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	set := func(path, value string) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: path, Value: value}
	}
	for _, test := range []struct {
		name  string
		specs []subdocSpec
	}{
		{name: "two sizes", specs: []subdocSpec{set("a", `"xxxx"`), set("a", `"yy"`)}},
		{name: "delete and set", specs: []subdocSpec{{Op: memd.SubDocOpDelete, Flags: xattrPath, Path: "a"}, set("a", `"yy"`)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := []byte(t.Name())
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: key, Value: []byte(`{}`), Extras: make([]byte, 8)})
			for _, spec := range []subdocSpec{set("a", `"xx"`), set("b", `1`)} {
				c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: key, Value: encodeMutationSpecs(spec)})
			}
			c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: key, Value: encodeMutationSpecs(test.specs...)})
			if status, got := lookupXattr(t, c, string(key), "$XTOC"); status != memd.StatusSuccess || got != `["a","b"]` {
				t.Errorf(`expected ["a","b"], got status 0x%02x value %s`, uint16(status), got)
			}
			if status, got := lookupXattr(t, c, string(key), "a"); status != memd.StatusSuccess || got != `"yy"` {
				t.Errorf(`expected "yy", got status 0x%02x value %s`, uint16(status), got)
			}
		})
	}

	t.Run("value as sent", func(t *testing.T) {
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("sent"), Value: []byte(`{}`), Extras: make([]byte, 8)})
		c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: []byte("sent"), Value: encodeMutationSpecs(set("x", `{"n": 2}`))})
		if status, got := lookupXattr(t, c, "sent", "x"); status != memd.StatusSuccess || got != `{"n": 2}` {
			t.Errorf(`expected {"n": 2}, got status 0x%02x value %s`, uint16(status), got)
		}
	})
}

// subjsonCase is a spec on a document; status is the spec's status, value
// its result, and doc the document after it, when given.
type subjsonCase struct {
	name   string
	doc    string
	lookup bool
	spec   subdocSpec
	status memd.StatusCode
	value  string
	want   string
}

func runSubjsonCase(t *testing.T, c *testKvConn, test subjsonCase) {
	t.Helper()

	key := []byte(t.Name())
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: key, Value: []byte(test.doc), Extras: make([]byte, 8)})
	var status memd.StatusCode
	var value string
	if test.lookup {
		resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiLookup, Vbucket: c.vbID, Key: key, Value: encodeLookupSpecs(test.spec)})
		if len(resp.Value) < 6 {
			t.Fatalf("lookup failed with status 0x%02x", uint16(resp.Status))
		}
		status, value = memd.StatusCode(binary.BigEndian.Uint16(resp.Value)), string(resp.Value[6:])
	} else {
		resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: key, Value: encodeMutationSpecs(test.spec)})
		switch {
		case resp.Status == memd.StatusSubDocBadMulti && len(resp.Value) == 3:
			status = memd.StatusCode(binary.BigEndian.Uint16(resp.Value[1:]))
		case resp.Status != memd.StatusSuccess:
			t.Fatalf("mutation failed with status 0x%02x", uint16(resp.Status))
		case len(resp.Value) >= 7:
			value = string(resp.Value[7:])
		}
	}
	if status != test.status {
		t.Errorf("expected spec status 0x%02x, got 0x%02x", uint16(test.status), uint16(status))
	}
	if value != test.value {
		t.Errorf("expected value %q, got %q", test.value, value)
	}
	got := string(c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: key}).Value)
	want := test.want
	if want == "" {
		want = test.doc
	}
	if got != want {
		t.Errorf("expected document %s, got %s", want, got)
	}
}

// subjson decodes the \u escapes of a document key before it compares the
// key with a path (HashKey::get_hk in subdoc/hkesc.h@v7.0.5), and keeps
// other escapes as written. Its decoder reads any "\u" as an escape, even
// one whose backslash is escaped.
func TestSubdocKeysWithUescapes(t *testing.T) {
	c := newTestKvConn(t)

	const doc = `{"\u00e9":2,"k\u0041":4,"é":5}`
	get := func(path string) subdocSpec { return subdocSpec{Op: memd.SubDocOpGet, Path: path} }
	for _, test := range []subjsonCase{
		{name: "get decoded key", doc: doc, lookup: true, spec: get("é"), value: "2"},
		{name: "get ascii", doc: doc, lookup: true, spec: get("kA"), value: "4"},
		{name: "dict add existing", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDictAdd, Path: "kA", Value: "1"}, status: memd.StatusSubDocPathExists},
		{name: "delete", doc: doc, spec: subdocSpec{Op: memd.SubDocOpDelete, Path: "é"}, want: `{"k\u0041":4,"é":5}`},
		{name: "counter", doc: doc, spec: subdocSpec{Op: memd.SubDocOpCounter, Path: "é", Value: "1"}, value: "3", want: `{"\u00e9":3,"k\u0041":4,"é":5}`},
		{name: "upsert under escaped key", doc: `{"\u00e9":{"x":1}}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Flags: mkdirP, Path: "é.x", Value: "2"}, want: `{"\u00e9":{"x":2}}`},
		{name: "push last", doc: `{"\u00e9":[1]}`, spec: subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "é", Value: "2"}, want: `{"\u00e9":[1,2]}`},
		{name: "escaped slash", doc: `{"a\u002fb":1,"a/b":2}`, lookup: true, spec: get("a/b"), value: "1"},
		{name: "surrogate pair", doc: `{"a\ud83d\ude00":1}`, lookup: true, spec: get("a😀"), value: "1"},
		{name: "escaped backslash", doc: `{"a\\u0041":1}`, lookup: true, spec: get(`a\\u0041`), status: memd.StatusSubDocPathNotFound},
		{name: "escaped quote", doc: `{"a\u0022b":1}`, lookup: true, spec: get(`a\"b`), status: memd.StatusSubDocPathNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			runSubjsonCase(t, c, test)
		})
	}
}

// subjson validates a value with its jsonsl parser (Validator::validate in
// subdoc/match.cc@v7.0.5), which does not require a comma between numbers in
// an array and takes whitespace for an empty list of values, so the document
// it writes may not be JSON. It refuses a document whose root is a string,
// and finds the last element of a scalar root as an empty value. Documents
// and values deeper than its parser allows are doc_too_deep and
// value_too_deep.
func TestSubdocFollowsJsonsl(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	deep := func(n int) string { return strings.Repeat("[", n) + strings.Repeat("]", n) }
	mutation := func(op memd.SubDocOpType, path, value string) subdocSpec {
		return subdocSpec{Op: op, Path: path, Value: value}
	}
	for _, test := range []subjsonCase{
		{name: "push last two values", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpArrayPushLast, "l", "1 2"), want: `{"l":[0,1 2]}`},
		{name: "insert two values", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpArrayInsert, "l[0]", "1 2"), want: `{"l":[1 2,0]}`},
		{name: "push last whitespace", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpArrayPushLast, "l", " "), want: `{"l":[0, ]}`},
		{name: "add unique whitespace", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpArrayAddUnique, "l", " "), want: `{"l":[0, ]}`},
		{name: "two strings", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpArrayPushLast, "l", `"a" "b"`), want: `{"l":[0,"a" "b"]}`},
		{name: "dict set two values", doc: `{"l":[0]}`, spec: mutation(memd.SubDocOpDictSet, "l", "1 2"), status: memd.StatusSubDocCantInsert},
		{name: "string root dict add", doc: `""`, spec: mutation(memd.SubDocOpDictAdd, "a", "1"), status: memd.StatusSubDocNotJSON},
		{name: "string root push last", doc: `"x]"`, spec: mutation(memd.SubDocOpArrayPushLast, "", "1"), status: memd.StatusSubDocNotJSON},
		{name: "string root counter", doc: `""`, spec: mutation(memd.SubDocOpCounter, "a", "1"), status: memd.StatusSubDocNotJSON},
		{name: "string root get", doc: `"x]"`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "[0]"}, status: memd.StatusSubDocNotJSON},
		{name: "number root last element", doc: `0`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "[-1]"}},
		{name: "null root last element", doc: `null`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "[-1]"}},
		{name: "number root element", doc: `0`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "[0]"}, status: memd.StatusSubDocPathMismatch},
		{name: "deep document get", doc: deep(33), lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "[0]"}, status: memd.StatusSubDocDocTooDeep},
		{name: "deep document push", doc: deep(33), spec: mutation(memd.SubDocOpArrayPushLast, "[0]", "1"), status: memd.StatusSubDocDocTooDeep},
		{name: "deep part after the match", doc: `{"a":1,"b":` + deep(40) + `}`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "a"}, value: "1"},
		{name: "deep part before the match", doc: `{"b":` + deep(40) + `,"a":1}`, lookup: true, spec: subdocSpec{Op: memd.SubDocOpGet, Path: "a"}, status: memd.StatusSubDocDocTooDeep},
		{name: "deepest value", doc: `{"a":1}`, spec: mutation(memd.SubDocOpDictSet, "b", deep(31)), want: `{"a":1,"b":` + deep(31) + `}`},
		{name: "deep value", doc: `{"a":1}`, spec: mutation(memd.SubDocOpDictSet, "b", deep(32)), status: memd.StatusSubDocValueTooDeep},
		{name: "deep value at depth", doc: `{"a":{"b":{}}}`, spec: mutation(memd.SubDocOpDictSet, "a.b.c", deep(31)), status: memd.StatusSubDocValueTooDeep},
		{name: "deep value pushed", doc: `{"a":[]}`, spec: mutation(memd.SubDocOpArrayPushLast, "a", deep(31)), status: memd.StatusSubDocValueTooDeep},
		{name: "deep xattr", doc: `{}`, spec: subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x", Value: deep(32)}, status: memd.StatusSubDocValueTooDeep},
	} {
		t.Run(test.name, func(t *testing.T) {
			runSubjsonCase(t, c, test)
		})
	}
}

// kv_engine decides a document's JSON datatype from its content whenever it
// stores a whole value, whatever the client sent (mutation_context.cc,
// appendprepend_context.cc@v7.0.5). Sub-document specs keep the datatype
// of the document they read, even when subjson wrote text that is not JSON,
// except that a whole-document set decides it again, and body specs run only
// on a JSON document (subdocument.cc@v7.0.5).
func TestSubdocBodyDatatype(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureSnappy, memd.FeatureJSON)

	do := func(t *testing.T, req *memd.Packet) *memd.Packet {
		t.Helper()
		req.Vbucket = c.vbID
		return c.Do(req)
	}
	must := func(t *testing.T, req *memd.Packet) {
		t.Helper()
		if resp := do(t, req); resp.Status != memd.StatusSuccess {
			t.Fatalf("%s failed with status 0x%02x", req.Command.Name(), uint16(resp.Status))
		}
	}
	store := func(cmd memd.CmdCode, key, value string, datatype memd.DatatypeFlag) *memd.Packet {
		return &memd.Packet{Command: cmd, Key: []byte(key), Value: []byte(value), Datatype: uint8(datatype), Extras: make([]byte, 8)}
	}
	mutate := func(key string, specs ...subdocSpec) *memd.Packet {
		return &memd.Packet{Command: memd.CmdSubDocMultiMutation, Key: []byte(key), Value: encodeMutationSpecs(specs...)}
	}
	upsert := func(path, value string) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpDictSet, Path: path, Value: value}
	}
	setDoc := func(value string) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpSetDoc, Value: value}
	}
	// check reads the document and a lookup of "a" on it.
	check := func(t *testing.T, key string, wantJSON bool) {
		t.Helper()
		get := do(t, &memd.Packet{Command: memd.CmdGet, Key: []byte(key)})
		if get.Status != memd.StatusSuccess {
			t.Fatalf("GET failed with status 0x%02x", uint16(get.Status))
		}
		if isJSON := get.Datatype&uint8(memd.DatatypeFlagJSON) != 0; isJSON != wantJSON {
			t.Errorf("expected JSON datatype %v, got datatype 0x%02x for %q", wantJSON, get.Datatype, get.Value)
		}
		resp := do(t, &memd.Packet{Command: memd.CmdSubDocMultiLookup, Key: []byte(key), Value: encodeLookupSpecs(subdocSpec{Op: memd.SubDocOpGet, Path: "a"})})
		if len(resp.Value) < 6 {
			t.Fatalf("lookup failed with status 0x%02x", uint16(resp.Status))
		}
		status := memd.StatusCode(binary.BigEndian.Uint16(resp.Value))
		if wantJSON && status == memd.StatusSubDocNotJSON || !wantJSON && status != memd.StatusSubDocNotJSON {
			t.Errorf("lookup on a document with JSON datatype %v got 0x%02x", wantJSON, uint16(status))
		}
	}

	for _, test := range []struct {
		name     string
		requests []*memd.Packet
		json     bool
	}{
		{name: "set valid without the bit", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0)}, json: true},
		{name: "set invalid with the bit", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}x`, memd.DatatypeFlagJSON)}},
		{name: "add invalid with the bit", requests: []*memd.Packet{store(memd.CmdAdd, "k", `{"a":1}x`, memd.DatatypeFlagJSON)}},
		{name: "replace invalid with the bit", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0), store(memd.CmdReplace, "k", `{"a":1}x`, memd.DatatypeFlagJSON)}},
		{name: "compressed invalid with the bit", requests: []*memd.Packet{{Command: memd.CmdSet, Key: []byte("k"), Value: snappy.Encode(nil, []byte(`{"a":1}x`)),
			Datatype: uint8(memd.DatatypeFlagJSON | memd.DatatypeFlagCompressed), Extras: make([]byte, 8)}}},
		{name: "append breaking JSON", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0), {Command: memd.CmdAppend, Key: []byte("k"), Value: []byte("x")}}},
		{name: "append making JSON", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1`, 0), {Command: memd.CmdAppend, Key: []byte("k"), Value: []byte("}")}}, json: true},
		{name: "lenient upsert", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0), mutate("k", upsert("b", "-01"))}, json: true},
		{name: "lenient upsert then append", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0), mutate("k", upsert("b", "-01")),
			{Command: memd.CmdAppend, Key: []byte("k"), Value: []byte(" ")}}},
		{name: "xattr mutation on a raw document", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}x`, 0),
			mutate("k", subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x", Value: "1"})}},
		{name: "whole document set that is not JSON", requests: []*memd.Packet{store(memd.CmdSet, "k", `{"a":1}`, 0), mutate("k", setDoc(`{"a":1}x`))}},
		{name: "whole document set then upsert on a raw document", requests: []*memd.Packet{store(memd.CmdSet, "k", "hello", 0),
			mutate("k", setDoc(`{"a":1}`), upsert("b", "2"))}, json: true},
		{name: "invalid utf-8", requests: []*memd.Packet{store(memd.CmdSet, "k", "{\"a\":\"\xff\"}", 0)}},
		{name: "overlong utf-8", requests: []*memd.Packet{store(memd.CmdSet, "k", "{\"a\":\"\xc0\x80\"}", 0)}, json: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			do(t, &memd.Packet{Command: memd.CmdDelete, Key: []byte("k")})
			for _, req := range test.requests {
				must(t, req)
			}
			check(t, "k", test.json)
		})
	}

	t.Run("whole document set that is not JSON then upsert", func(t *testing.T) {
		must(t, store(memd.CmdSet, "k", `{"a":1}`, 0))
		expectSpecError(t, do(t, mutate("k", setDoc(`{"b":1}x`), upsert("b", "2"))), 1, memd.StatusSubDocNotJSON)
	})
}

// kv_engine creates a document for Mkdoc or Add from a JSON template whose
// root it takes from the first sub-document spec, xattr or not
// (subdocument_context.cc@v7.0.5), also over a deleted or expired document
// it does not see.
func TestSubdocMkdocTemplate(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureJSON)

	mkdoc := func(t *testing.T, key string, specs ...subdocSpec) {
		t.Helper()
		resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: []byte(key),
			Extras: []byte{uint8(memd.SubdocDocFlagMkDoc)}, Value: encodeMutationSpecs(specs...)})
		if resp.Status != memd.StatusSuccess {
			t.Fatalf("mutation failed with status 0x%02x", uint16(resp.Status))
		}
	}
	expect := func(t *testing.T, key, want string) {
		t.Helper()
		get := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte(key)})
		if get.Status != memd.StatusSuccess || string(get.Value) != want || get.Datatype&uint8(memd.DatatypeFlagJSON) == 0 {
			t.Errorf("expected JSON %s, got status 0x%02x datatype 0x%02x value %s", want, uint16(get.Status), get.Datatype, get.Value)
		}
	}
	upsert := subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}

	t.Run("xattr only", func(t *testing.T) {
		mkdoc(t, "xattr", subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x.y", Value: "1"})
		expect(t, "xattr", `{}`)
		mkdoc(t, "xattr", upsert)
		expect(t, "xattr", `{"a":1}`)
	})
	t.Run("array path", func(t *testing.T) {
		mkdoc(t, "array", subdocSpec{Op: memd.SubDocOpArrayPushLast, Path: "", Value: "1"})
		expect(t, "array", `[1]`)
	})
	t.Run("deleted", func(t *testing.T) {
		c.Do(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("deleted"), Value: []byte(`{"b":1}`), Extras: make([]byte, 8)})
		c.Do(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("deleted")})
		mkdoc(t, "deleted", upsert)
		expect(t, "deleted", `{"a":1}`)
	})
	t.Run("expired", func(t *testing.T) {
		withExpiry := make([]byte, 8)
		binary.BigEndian.PutUint32(withExpiry[4:], 10)
		c.Do(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("expired"), Value: []byte(`{"b":1}`), Extras: withExpiry})
		c.cluster.Chrono().TimeTravel(20 * time.Second)
		mkdoc(t, "expired", upsert)
		expect(t, "expired", `{"a":1}`)
	})
}

// kv_engine keeps a counter's flags when it changes its value
// (arithmetic_context.cc@v7.0.5).
func TestCounterKeepsFlags(t *testing.T) {
	c := newTestKvConn(t)

	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, 0xcafe)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("counter"), Value: []byte("1"), Extras: extras})
	incr := make([]byte, 20)
	binary.BigEndian.PutUint64(incr, 1)
	c.mustSucceed(&memd.Packet{Command: memd.CmdIncrement, Vbucket: c.vbID, Key: []byte("counter"), Extras: incr})
	get := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("counter")})
	if flags := binary.BigEndian.Uint32(get.Extras); flags != 0xcafe || string(get.Value) != "2" {
		t.Errorf("expected value 2 with flags 0xcafe, got %s with 0x%x", get.Value, flags)
	}
}

// Add implies create path for every spec (subdocument_context.cc@v7.0.5),
// and subjson has no create-path form of replace or array insert: once the
// path parses it answers GLOBAL_ENOSUPPORT, which kv_engine sends as an
// internal error for that spec (subdocument.cc@v7.0.5).
func TestSubdocAddWithReplaceOrInsert(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	insert := func(path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpArrayInsert, Flags: flags, Path: path, Value: "1"}
	}
	replace := func(path string, flags memd.SubdocFlag) subdocSpec {
		return subdocSpec{Op: memd.SubDocOpReplace, Flags: flags, Path: path, Value: "1"}
	}
	for _, test := range []struct {
		name  string
		specs []subdocSpec
		index uint8
	}{
		{name: "insert at root", specs: []subdocSpec{insert("[0]", 0)}},
		{name: "insert", specs: []subdocSpec{insert("a[0]", 0)}},
		{name: "replace", specs: []subdocSpec{replace("a", 0)}},
		{name: "replace at root", specs: []subdocSpec{replace("[0]", 0)}},
		{name: "xattr replace", specs: []subdocSpec{replace("x.y", xattrPath)}},
		{name: "after xattr", specs: []subdocSpec{{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "x.y", Value: "1"}, insert("[0]", 0)}, index: 1},
		{name: "after whole document", specs: []subdocSpec{{Op: memd.SubDocOpSetDoc, Value: "[2]"}, insert("[0]", 0)}, index: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := []byte(t.Name())
			resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: key,
				Extras: []byte{uint8(memd.SubdocDocFlagAddDoc)}, Value: encodeMutationSpecs(test.specs...)})
			expectSpecError(t, resp, test.index, memd.StatusInternalError)
			if get := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: key}); get.Status != memd.StatusKeyNotFound {
				t.Errorf("failed request stored a document: status 0x%02x", uint16(get.Status))
			}
		})
	}
}

// Mkdoc or Add over a deleted or expired document the request cannot see
// creates a new document: the template body, no flags and none of the old
// xattrs, system ones included.
func TestSubdocMkdocReplacesInvisibleDocument(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	mutate := func(t *testing.T, key string, extras []byte, specs ...subdocSpec) {
		t.Helper()
		resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: []byte(key),
			Extras: extras, Value: encodeMutationSpecs(specs...)})
		if resp.Status != memd.StatusSuccess {
			t.Fatalf("mutation failed with status 0x%02x", uint16(resp.Status))
		}
	}
	run := func(t *testing.T, key string, docFlags memd.SubdocDocFlag, specs ...subdocSpec) {
		t.Helper()
		mutate(t, key, []byte{uint8(docFlags)}, specs...)
	}
	// prepare stores a document with flags, a user and a system xattr, the
	// last mutation giving it the expiry.
	prepare := func(t *testing.T, key string, expiry uint32) {
		t.Helper()
		extras := make([]byte, 8)
		binary.BigEndian.PutUint32(extras, 7)
		if resp := c.Do(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte(key), Value: []byte(`{"b":1}`), Extras: extras}); resp.Status != memd.StatusSuccess {
			t.Fatalf("SET failed with status 0x%02x", uint16(resp.Status))
		}
		run(t, key, 0, subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "u", Value: "1"})
		withExpiry := make([]byte, 4)
		binary.BigEndian.PutUint32(withExpiry, expiry)
		mutate(t, key, withExpiry, subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath, Path: "_s", Value: "2"})
	}
	for _, test := range []struct {
		name     string
		docFlags memd.SubdocDocFlag
		expire   bool
	}{
		{name: "deleted mkdoc", docFlags: memd.SubdocDocFlagMkDoc},
		{name: "deleted add", docFlags: memd.SubdocDocFlagAddDoc},
		{name: "expired mkdoc", docFlags: memd.SubdocDocFlagMkDoc, expire: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := t.Name()
			if test.expire {
				prepare(t, key, 10)
				c.cluster.Chrono().TimeTravel(20 * time.Second)
			} else {
				prepare(t, key, 0)
				c.Do(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte(key)})
			}
			run(t, key, test.docFlags, subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"})

			get := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte(key)})
			if get.Status != memd.StatusSuccess || string(get.Value) != `{"a":1}` || binary.BigEndian.Uint32(get.Extras) != 0 {
				t.Errorf("expected {\"a\":1} with no flags, got status 0x%02x value %s extras %v", uint16(get.Status), get.Value, get.Extras)
			}
			for _, path := range []string{"u", "_s"} {
				if status, value := lookupXattr(t, c, key, path); status != memd.StatusSubDocPathNotFound {
					t.Errorf("xattr %s survived: status 0x%02x value %s", path, uint16(status), value)
				}
			}
			run(t, key, memd.SubdocDocFlagMkDoc, subdocSpec{Op: memd.SubDocOpDictSet, Flags: xattrPath | mkdirP, Path: "_s.z", Value: "3"})
		})
	}
}
