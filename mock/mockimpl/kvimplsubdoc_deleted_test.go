package mockimpl

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

const (
	statusDeletedDocumentCantHaveValue = memd.StatusCode(0xd7)
	statusUnknownVattrMacro            = memd.StatusCode(0xd5)
)

func (c *testKvConn) mutateIn(key string, cas uint64, extras []byte, specs ...subdocSpec) *memd.Packet {
	c.t.Helper()

	return c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte(key),
		Cas:     cas,
		Extras:  extras,
		Value:   encodeMutationSpecs(specs...),
	})
}

func docFlags(flags memd.SubdocDocFlag) []byte {
	return []byte{uint8(flags)}
}

// lookupDeleted runs one lookup spec with AccessDeleted and returns the
// response status, the spec status and the spec value.
func (c *testKvConn) lookupDeleted(key string, op memd.SubDocOpType, flags memd.SubdocFlag, path string) (memd.StatusCode, memd.StatusCode, []byte) {
	c.t.Helper()

	spec := encodeLookupSpec(op, path)
	spec[1] = uint8(flags)
	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiLookup,
		Vbucket: c.vbID,
		Key:     []byte(key),
		Extras:  docFlags(memd.SubdocDocFlagAccessDeleted),
		Value:   spec,
	})
	if len(resp.Value) < 6 {
		return resp.Status, 0, nil
	}
	return resp.Status, memd.StatusCode(binary.BigEndian.Uint16(resp.Value)), resp.Value[6:]
}

// xattr returns an xattr of a document, deleted or not, or nil if it has
// none.
func (c *testKvConn) xattr(key, name string) []byte {
	c.t.Helper()

	_, status, value := c.lookupDeleted(key, memd.SubDocOpGet, memd.SubdocFlagXattrPath, name)
	if status != memd.StatusSuccess {
		return nil
	}
	return value
}

func (c *testKvConn) datatype(key string) string {
	c.t.Helper()

	return string(c.xattr(key, "$document.datatype"))
}

func xattrSpec(path, value string) subdocSpec {
	return subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP, Path: path, Value: value}
}

var setMetaField = xattrSpec("meta.field", `"b"`)

const testDocFlags = 0xdeadbeef

type deletedTestState int

const (
	stateMissing deletedTestState = iota
	stateLive
	stateDeleted        // removed by DELETE, which keeps only system xattrs
	stateCreatedDeleted // created by CreateAsDeleted with both xattrs
	stateExpired
)

func (s deletedTestState) String() string {
	return [...]string{"missing", "live", "deleted", "created deleted", "expired"}[s]
}

// prepare stores key in state s. Every state but missing has the system
// xattr _old and, unless DELETE pruned it, the user xattr old.
func (c *testKvConn) prepare(key string, s deletedTestState) {
	c.t.Helper()

	switch s {
	case stateMissing:
		return
	case stateCreatedDeleted:
		expectStatus(c.t, "create deleted", c.mutateIn(key, 0, docFlags(memd.SubdocDocFlagMkDoc|memd.SubdocDocFlagCreateAsDeleted), xattrSpec("_old.x", "1")), memd.StatusSubDocSuccessDeleted)
		expectStatus(c.t, "update deleted", c.mutateIn(key, 0, docFlags(memd.SubdocDocFlagAccessDeleted), xattrSpec("old.x", "1")), memd.StatusSubDocSuccessDeleted)
		return
	}

	setExtras := make([]byte, 8)
	binary.BigEndian.PutUint32(setExtras, testDocFlags)
	var mutateExtras []byte
	if s == stateExpired {
		binary.BigEndian.PutUint32(setExtras[4:], 10)
		mutateExtras = []byte{0, 0, 0, 10}
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte(key), Value: []byte(`{"a":1}`), Extras: setExtras})
	expectStatus(c.t, "set _old", c.mutateIn(key, 0, mutateExtras, xattrSpec("_old.x", "1")), memd.StatusSuccess)
	expectStatus(c.t, "set old", c.mutateIn(key, 0, mutateExtras, xattrSpec("old.x", "1")), memd.StatusSuccess)

	switch s {
	case stateDeleted:
		c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte(key)})
	case stateExpired:
		c.cluster.Chrono().TimeTravel(20 * time.Second)
	}
}

func expectStatus(t *testing.T, what string, resp *memd.Packet, status memd.StatusCode) {
	t.Helper()

	if resp.Status != status {
		t.Fatalf("%s: expected status 0x%02x, got 0x%02x", what, uint16(status), uint16(resp.Status))
	}
}

// TestMultiMutationDocumentStates sets an xattr with every combination of
// Mkdoc or Add, CreateAsDeleted and AccessDeleted on a document in each
// state.
func TestMultiMutationDocumentStates(t *testing.T) {
	const oldX = `{"x":1}`

	for _, state := range []deletedTestState{stateMissing, stateLive, stateDeleted, stateCreatedDeleted, stateExpired} {
		for _, flags := range []memd.SubdocDocFlag{
			memd.SubdocDocFlagMkDoc,
			memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAccessDeleted,
			memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagCreateAsDeleted,
			memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagCreateAsDeleted | memd.SubdocDocFlagAccessDeleted,
			memd.SubdocDocFlagAddDoc,
			memd.SubdocDocFlagAddDoc | memd.SubdocDocFlagAccessDeleted,
			memd.SubdocDocFlagAddDoc | memd.SubdocDocFlagCreateAsDeleted,
			memd.SubdocDocFlagAddDoc | memd.SubdocDocFlagCreateAsDeleted | memd.SubdocDocFlagAccessDeleted,
		} {
			add := flags&memd.SubdocDocFlagAddDoc != 0
			createAsDeleted := flags&memd.SubdocDocFlagCreateAsDeleted != 0
			accessDeleted := flags&memd.SubdocDocFlagAccessDeleted != 0

			// Without AccessDeleted kv_engine does not see a deleted or
			// expired document, so Mkdoc and Add create a new one, with no
			// xattrs and no flags, stored with Add semantics. An Add of a
			// deleted document fails over any other, deleted or not
			// (VBucket::processAdd): Mkdoc retries until it gives up with
			// temporary_failure, and Add answers not_stored.
			visible := state == stateLive || (state != stateMissing && accessDeleted)
			status := memd.StatusSuccess
			deleted := state != stateLive
			wantOld, wantUserOld := oldX, oldX
			if state == stateDeleted || state == stateExpired {
				wantUserOld = ""
			}
			switch {
			case add && visible:
				status = memd.StatusKeyExists
			case !visible && createAsDeleted && state != stateMissing:
				status = memd.StatusTmpFail
				if add {
					status = memd.StatusNotStored
				}
			case !visible:
				deleted = createAsDeleted
				wantOld, wantUserOld = "", ""
			}
			if deleted && status == memd.StatusSuccess {
				status = memd.StatusSubDocSuccessDeleted
			}

			t.Run(fmt.Sprintf("%s/0x%02x", state, uint8(flags)), func(t *testing.T) {
				c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted, memd.FeatureJSON)
				c.prepare("doc", state)

				resp := c.mutateIn("doc", 0, docFlags(flags), setMetaField)
				expectStatus(t, "mutate", resp, status)
				switch status {
				case memd.StatusKeyExists:
					return
				case memd.StatusTmpFail, memd.StatusNotStored:
					if meta := c.xattr("doc", "meta"); meta != nil {
						t.Errorf("expected nothing stored, got xattr meta %s", meta)
					}
					if got := string(c.xattr("doc", "_old")); got != wantOld {
						t.Errorf("expected xattr _old %q kept, got %q", wantOld, got)
					}
					if got := string(c.xattr("doc", "old")); got != wantUserOld {
						t.Errorf("expected xattr old %q, got %q", wantUserOld, got)
					}
					return
				}

				resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
				wantDatatype := `["xattr"]`
				if deleted {
					expectStatus(t, "get", resp, memd.StatusKeyNotFound)
				} else {
					expectStatus(t, "get", resp, memd.StatusSuccess)
					// A document created live gets a root from the xattr path.
					wantFlags, wantBody := uint32(0), `{}`
					if visible {
						wantFlags, wantBody = testDocFlags, `{"a":1}`
					}
					if gotFlags := binary.BigEndian.Uint32(resp.Extras); gotFlags != wantFlags {
						t.Errorf("expected flags 0x%08x, got 0x%08x", wantFlags, gotFlags)
					}
					if string(resp.Value) != wantBody || resp.Datatype != uint8(memd.DatatypeFlagJSON) {
						t.Errorf("expected JSON body %s, got %s with datatype 0x%02x", wantBody, resp.Value, resp.Datatype)
					}
					wantDatatype = `["json","xattr"]`
				}
				if got := c.datatype("doc"); got != wantDatatype {
					t.Errorf("expected $document.datatype %s, got %s", wantDatatype, got)
				}

				lookupStatus, _, meta := c.lookupDeleted("doc", memd.SubDocOpGet, memd.SubdocFlagXattrPath, "meta")
				wantLookupStatus := memd.StatusSuccess
				if deleted {
					wantLookupStatus = memd.StatusSubDocSuccessDeleted
				}
				if lookupStatus != wantLookupStatus || string(meta) != `{"field":"b"}` {
					t.Errorf("expected xattr meta with status 0x%02x, got %s with status 0x%02x", uint16(wantLookupStatus), meta, uint16(lookupStatus))
				}
				if got := string(c.xattr("doc", "_old")); got != wantOld {
					t.Errorf("expected xattr _old %q, got %q", wantOld, got)
				}
				if got := string(c.xattr("doc", "old")); got != wantUserOld {
					t.Errorf("expected xattr old %q, got %q", wantUserOld, got)
				}
			})
		}
	}
}

func TestMultiMutationCreateAsDeletedWithBodyOnLiveDocument(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})

	resp := c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagMkDoc|memd.SubdocDocFlagCreateAsDeleted),
		setMetaField, subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "2"})
	expectStatus(t, "mutate", resp, memd.StatusSuccess)

	resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	if string(resp.Value) != `{"a":1,"b":2}` {
		t.Errorf("expected the live document to be updated, got %s", resp.Value)
	}
}

// TestMultiMutationDeleteDocReportsLiveDocument checks that the response
// reports the state of the document the specs ran on, as kv_engine does, not
// the state a DeleteDoc spec stores.
func TestMultiMutationDeleteDocReportsLiveDocument(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
	expectStatus(t, "set user xattr", c.mutateIn("doc", 0, nil, xattrSpec("user.x", "1")), memd.StatusSuccess)

	resp := c.mutateIn("doc", 0, nil, xattrSpec("_sys.x", "1"), subdocSpec{Op: memd.SubDocOpDeleteDoc})
	expectStatus(t, "delete", resp, memd.StatusSuccess)

	resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
	expectStatus(t, "get", resp, memd.StatusKeyNotFound)

	// kv_engine removes the body and the user xattrs of the document.
	if sys := c.xattr("doc", "_sys"); string(sys) != `{"x":1}` {
		t.Errorf("expected system xattr _sys to be kept, got %s", sys)
	}
	if user := c.xattr("doc", "user"); user != nil {
		t.Errorf("expected user xattr user to be removed, got %s", user)
	}
	if size := c.xattr("doc", "$document.value_bytes"); string(size) != "0" {
		t.Errorf("expected the body to be removed, got $document.value_bytes %s", size)
	}
	if got := c.datatype("doc"); got != `["xattr"]` {
		t.Errorf("expected $document.datatype [\"xattr\"], got %s", got)
	}

	// The deleted document has no body, so an xattr can be set on it.
	resp = c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagAccessDeleted), xattrSpec("_sys.y", "2"))
	expectStatus(t, "update deleted", resp, memd.StatusSubDocSuccessDeleted)
	if size := c.xattr("doc", "$document.value_bytes"); string(size) != "0" {
		t.Errorf("expected no body after the update, got $document.value_bytes %s", size)
	}
}

// TestWholeBodyOnDeletedDocumentRefused checks that a whole-document set is
// refused as invalid arguments with CreateAsDeleted or AccessDeleted, which
// its spec does not accept (subdocument_traits.h), before anything is stored.
func TestWholeBodyOnDeletedDocumentRefused(t *testing.T) {
	setBody := subdocSpec{Op: memd.SubDocOpSetDoc, Value: `{"a":1}`}

	for _, test := range []struct {
		name  string
		state deletedTestState
		flags memd.SubdocDocFlag
	}{
		{"create as deleted", stateMissing, memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagCreateAsDeleted},
		{"access deleted", stateDeleted, memd.SubdocDocFlagAccessDeleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)
			c.prepare("doc", test.state)

			expectStatus(t, "mutate", c.mutateIn("doc", 0, docFlags(test.flags), setMetaField, setBody), memd.StatusInvalidArgs)
			if meta := c.xattr("doc", "meta"); meta != nil {
				t.Errorf("expected nothing to be stored, got xattr meta %s", meta)
			}
		})
	}
}

// TestReplaceBodyWithXattr follows kv_engine's ReplaceBodyWithXattr spec: the
// value of the xattr path becomes the body, a deleted document keeps no body
// unless it is revived, and later body specs see the new body.
func TestReplaceBodyWithXattr(t *testing.T) {
	replace := subdocSpec{Op: memd.SubDocOpReplaceBodyWithXattr, Flags: memd.SubdocFlagXattrPath, Path: "txn.stgd"}
	removeTxn := subdocSpec{Op: memd.SubDocOpDelete, Flags: memd.SubdocFlagXattrPath, Path: "txn"}
	const (
		access = memd.SubdocDocFlagAccessDeleted
		revive = memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagReviveDocument
	)

	for _, test := range []struct {
		name    string
		deleted bool
		flags   memd.SubdocDocFlag
		specs   []subdocSpec
		status  memd.StatusCode
		body    string
	}{
		{"live", false, 0, []subdocSpec{replace, removeTxn}, memd.StatusSuccess, `{"b":2}`},
		{"revived", true, revive, []subdocSpec{replace, removeTxn}, memd.StatusSuccess, `{"b":2}`},
		{"revived with a body spec", true, revive, []subdocSpec{replace, {Op: memd.SubDocOpDictSet, Path: "c", Value: "3"}}, memd.StatusSuccess, `{"b":2,"c":3}`},
		{"deleted", true, access, []subdocSpec{replace}, statusDeletedDocumentCantHaveValue, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted, memd.FeatureJSON)
			stage := xattrSpec("txn.stgd", `{"b":2}`)
			if test.deleted {
				expectStatus(t, "stage", c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagMkDoc|memd.SubdocDocFlagCreateAsDeleted), stage), memd.StatusSubDocSuccessDeleted)
			} else {
				c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
				expectStatus(t, "stage", c.mutateIn("doc", 0, nil, stage), memd.StatusSuccess)
			}

			resp := c.mutateIn("doc", 0, docFlags(test.flags), test.specs...)
			expectStatus(t, "replace", resp, test.status)
			if test.status == statusDeletedDocumentCantHaveValue {
				expectErrorContext(t, resp, "A deleted document can't have a value")
			}
			if test.status != memd.StatusSuccess {
				if txn := c.xattr("doc", "txn"); string(txn) != `{"stgd":{"b":2}}` {
					t.Errorf("expected the document to be unchanged, got xattr txn %s", txn)
				}
				return
			}
			if len(resp.Value) != 0 {
				t.Errorf("expected no spec values, got %v", resp.Value)
			}

			resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
			if string(resp.Value) != test.body || resp.Datatype != uint8(memd.DatatypeFlagJSON) {
				t.Errorf("expected JSON body %s, got %s with datatype 0x%02x", test.body, resp.Value, resp.Datatype)
			}
		})
	}

	t.Run("missing xattr", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})

		expectSpecError(t, c.mutateIn("doc", 0, nil, replace), 0, memd.StatusSubDocPathNotFound)
	})

	// $document describes the document as read, before the spec replaced
	// its body.
	t.Run("document macro after replace", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
		expectStatus(t, "stage", c.mutateIn("doc", 0, nil, xattrSpec("txn.stgd", `{"b":2}`)), memd.StatusSuccess)

		read := subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros, Path: "txn.read", Value: `"${$document.value_bytes}"`}
		expectStatus(t, "replace", c.mutateIn("doc", 0, nil, replace, read), memd.StatusSuccess)
		if got := c.xattr("doc", "txn.read"); string(got) != "7" {
			t.Errorf("expected the size of the body read, 7, got %s", got)
		}
	})
}

// TestValueCrc32cMacro checks that ${Mutation.value_crc32c} is the CRC32C
// of the body stored, as eight hex digits: the CRC32C of {"a":1,"b":46} is
// 0x00fb9cde.
func TestValueCrc32cMacro(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})

	resp := c.mutateIn("doc", 0, nil,
		macroSpec("meta.stored", `"${Mutation.value_crc32c}"`),
		subdocSpec{Op: memd.SubDocOpDictSet, Path: "b", Value: "46"})
	expectStatus(t, "mutate", resp, memd.StatusSuccess)

	if got := c.xattr("doc", "meta.stored"); string(got) != `"0x00fb9cde"` {
		t.Errorf(`expected xattr meta.stored "0x00fb9cde", got %s`, got)
	}
}

func macroSpec(path, value string) subdocSpec {
	return subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagMkDirP | memd.SubdocFlagExpandMacros, Path: path, Value: value}
}

// TestDocumentMacros checks the ${$document} macros against the $document
// virtual attribute of the document read, formatted as kv_engine's
// expand_virtual_document_macro does, and that any other field, such as
// value_crc32c, fails its spec with subdoc_xattr_unknown_vattr_macro, as
// last_modified does until the vbucket has persisted a mutation and its CAS
// counts as a hybrid logical clock.
func TestDocumentMacros(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, 42)
	binary.BigEndian.PutUint32(extras[4:], 100)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: extras})
	expectStatus(t, "set xattr", c.mutateIn("doc", 0, nil, xattrSpec("old", "1")), memd.StatusSuccess)

	if _, ok := docVattr(t, c)["last_modified"]; ok {
		t.Errorf("expected no last_modified before the vbucket persists")
	}
	expectSpecError(t, c.mutateIn("doc", 0, nil, macroSpec("m.lm", `"${$document.last_modified}"`)), 0, statusUnknownVattrMacro)
	c.cluster.Chrono().TimeTravel(time.Second)

	read := docVattr(t, c)
	fields := []string{"CAS", "vbucket_uuid", "seqno", "revid", "exptime", "flags", "value_bytes", "datatype", "deleted", "last_modified"}
	specs := []subdocSpec{macroSpec("m.document", `"${$document}"`)}
	for _, field := range fields {
		specs = append(specs, macroSpec("m."+field, `"${$document.`+field+`}"`))
	}
	expectStatus(t, "mutate", c.mutateIn("doc", 0, nil, specs...), memd.StatusSuccess)

	for _, field := range fields {
		want := string(read[field])
		// kv_engine expands last_modified as a number.
		if field == "last_modified" {
			want = strings.Trim(want, `"`)
		}
		if got := c.xattr("doc", "m."+field); string(got) != want {
			t.Errorf("expected ${$document.%s} %s, got %s", field, want, got)
		}
	}
	var whole map[string]json.RawMessage
	if err := json.Unmarshal(c.xattr("doc", "m.document"), &whole); err != nil || !reflect.DeepEqual(whole, read) {
		t.Errorf("expected ${$document} %v, got %v (%v)", read, whole, err)
	}

	resp := c.mutateIn("doc", 0, nil, macroSpec("m.crc", `"${$document.value_crc32c}"`))
	expectSpecError(t, resp, 0, statusUnknownVattrMacro)
}

func docVattr(t *testing.T, c *testKvConn) map[string]json.RawMessage {
	t.Helper()

	var vattr map[string]json.RawMessage
	if err := json.Unmarshal(c.xattr("doc", "$document"), &vattr); err != nil {
		t.Fatalf("failed to read $document: %v", err)
	}
	return vattr
}

// TestFirstFailingSpecIsXattr checks that, with the xattr specs run first
// as kv_engine runs them, the failure of an xattr spec is reported before
// that of a later body spec.
func TestFirstFailingSpecIsXattr(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})

	resp := c.mutateIn("doc", 0, nil,
		subdocSpec{Op: memd.SubDocOpReplace, Flags: memd.SubdocFlagXattrPath, Path: "x.missing", Value: "1"},
		subdocSpec{Op: memd.SubDocOpReplace, Path: "missing", Value: "1"})
	expectSpecError(t, resp, 0, memd.StatusSubDocPathNotFound)
}

// TestSeqNoMacro checks that ${Mutation.seqno} is the sequence number of the
// mutation, as kv_engine's pre_link_document writes it.
func TestSeqNoMacro(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})

	spec := subdocSpec{Op: memd.SubDocOpDictSet, Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros, Path: "seq", Value: `"${Mutation.seqno}"`}
	expectStatus(t, "mutate", c.mutateIn("doc", 0, nil, spec), memd.StatusSuccess)

	seqNo := c.xattr("doc", "$document.seqno")
	if got := c.xattr("doc", "seq"); len(seqNo) != len(`"0x0000000000000000"`) || string(got) != string(seqNo) {
		t.Errorf("expected xattr seq %s, got %s", seqNo, got)
	}
}

// TestExpiryIsAMutation checks that a document that expired is deleted by a
// mutation of its own, with a new CAS and revision, as kv_engine's
// VBucket::processExpiredItem does, so the CAS read before expiry no longer
// matches.
func TestExpiryIsAMutation(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)

	withExpiry := make([]byte, 8)
	binary.BigEndian.PutUint32(withExpiry[4:], 10)
	cas := c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: withExpiry}).Cas
	revID := string(c.xattr("doc", "$document.revid"))
	c.cluster.Chrono().TimeTravel(20 * time.Second)

	resp := c.Do(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("doc"),
		Cas:     cas,
		Extras:  docFlags(memd.SubdocDocFlagAccessDeleted),
		Value:   encodeMutationSpecs(setMetaField),
	})
	expectStatus(t, "mutate with the old CAS", resp, memd.StatusKeyExists)

	if got := c.xattr("doc", "$document.CAS"); string(got) == fmt.Sprintf(`"0x%016x"`, cas) {
		t.Errorf("expected a new CAS, got the old one %s", got)
	}
	if got := string(c.xattr("doc", "$document.revid")); got == revID {
		t.Errorf("expected a new revision after %s, got %s", revID, got)
	}
}

// TestMultiMutationReviveDocument follows kv_engine's ReviveDocument: only a
// deleted document is revived, it is stored live with its xattrs, and the
// response reports it live.
func TestMultiMutationReviveDocument(t *testing.T) {
	const revive = memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagReviveDocument

	t.Run("deleted", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted, memd.FeatureJSON)
		c.prepare("doc", stateCreatedDeleted)

		expectStatus(t, "revive", c.mutateIn("doc", 0, docFlags(revive), setMetaField), memd.StatusSuccess)

		resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
		if len(resp.Value) != 0 || resp.Datatype != 0 {
			t.Errorf("expected an empty raw body, got %q with datatype 0x%02x", resp.Value, resp.Datatype)
		}
		for name, want := range map[string]string{"meta": `{"field":"b"}`, "_old": `{"x":1}`, "old": `{"x":1}`} {
			if got := c.xattr("doc", name); string(got) != want {
				t.Errorf("expected xattr %s %s, got %s", name, want, got)
			}
		}
		if status, _, _ := c.lookupDeleted("doc", memd.SubDocOpGet, memd.SubdocFlagXattrPath, "meta"); status != memd.StatusSuccess {
			t.Errorf("expected a lookup of a live document, got status 0x%02x", uint16(status))
		}
	})

	// A deleted document is stored with Add semantics, which fails over the
	// deleted document there, so nothing is stored.
	t.Run("deleted by a spec", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)
		c.prepare("doc", stateCreatedDeleted)

		resp := c.mutateIn("doc", 0, docFlags(revive), setMetaField, subdocSpec{Op: memd.SubDocOpDeleteDoc})
		expectStatus(t, "revive", resp, memd.StatusNotStored)
		if meta := c.xattr("doc", "meta"); meta != nil {
			t.Errorf("expected nothing stored, got xattr meta %s", meta)
		}
		if old := c.xattr("doc", "old"); string(old) != `{"x":1}` {
			t.Errorf("expected user xattr old kept, got %s", old)
		}
	})

	t.Run("body path", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.prepare("doc", stateDeleted)

		resp := c.mutateIn("doc", 0, docFlags(revive), subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"})
		expectSpecError(t, resp, 0, memd.StatusSubDocNotJSON)
		resp = c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
		expectStatus(t, "get", resp, memd.StatusKeyNotFound)
	})

	for _, test := range []struct {
		name   string
		state  deletedTestState
		flags  memd.SubdocDocFlag
		status memd.StatusCode
	}{
		{"live", stateLive, revive, memd.StatusSubDocCanOnlyReviveDeletedDocuments},
		{"missing with mkdoc", stateMissing, revive | memd.SubdocDocFlagMkDoc, memd.StatusSubDocCanOnlyReviveDeletedDocuments},
		{"deleted with add", stateDeleted, revive | memd.SubdocDocFlagAddDoc, memd.StatusKeyExists},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			c.prepare("doc", test.state)

			expectStatus(t, "revive", c.mutateIn("doc", 0, docFlags(test.flags), setMetaField), test.status)
			if meta := c.xattr("doc", "meta"); meta != nil {
				t.Errorf("expected nothing to be stored, got xattr meta %s", meta)
			}
		})
	}

	t.Run("lookup", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.prepare("live", stateLive)
		c.prepare("deleted", stateDeleted)

		for key, status := range map[string]memd.StatusCode{
			"live":    memd.StatusSubDocCanOnlyReviveDeletedDocuments,
			"deleted": memd.StatusSubDocSuccessDeleted,
		} {
			spec := encodeLookupSpec(memd.SubDocOpGet, "_old")
			spec[1] = uint8(memd.SubdocFlagXattrPath)
			resp := c.Do(&memd.Packet{Command: memd.CmdSubDocMultiLookup, Vbucket: c.vbID, Key: []byte(key), Extras: docFlags(revive), Value: spec})
			expectStatus(t, key, resp, status)
		}
		c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("live")})
		resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("deleted")})
		expectStatus(t, "get", resp, memd.StatusKeyNotFound)
	})
}

// TestDocumentDatatype checks the datatype kv_engine keeps for the body:
// raw for a deleted document and for a raw body under xattrs, and the
// validity of the body a whole-document spec leaves.
func TestDocumentDatatype(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureJSON)

	// kv_engine sets the JSON datatype of a stored value by its content.
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("set"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)})
	if resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("set")}); resp.Datatype != uint8(memd.DatatypeFlagJSON) {
		t.Errorf("set: expected datatype 0x01, got 0x%02x", resp.Datatype)
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdAppend, Vbucket: c.vbID, Key: []byte("set"), Value: []byte("x")})
	if resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("set")}); resp.Datatype != 0 {
		t.Errorf("append: expected datatype 0x00, got 0x%02x", resp.Datatype)
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("raw"), Value: []byte("text"), Extras: make([]byte, 8)})
	expectStatus(t, "set xattr", c.mutateIn("raw", 0, nil, setMetaField), memd.StatusSuccess)
	if resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("raw")}); resp.Datatype != 0 {
		t.Errorf("raw: expected datatype 0x00, got 0x%02x", resp.Datatype)
	}
	if got := c.datatype("raw"); got != `["xattr"]` {
		t.Errorf("raw: expected $document.datatype [\"xattr\"], got %s", got)
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("json"), Value: []byte(`{}`), Extras: make([]byte, 8), Datatype: uint8(memd.DatatypeFlagJSON)})
	expectStatus(t, "set body", c.mutateIn("json", 0, nil, subdocSpec{Op: memd.SubDocOpSetDoc, Value: "text"}), memd.StatusSuccess)
	if resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("json")}); resp.Datatype != 0 {
		t.Errorf("json: expected datatype 0x00 after a raw body, got 0x%02x", resp.Datatype)
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("deleted"), Value: []byte(`{}`), Extras: make([]byte, 8), Datatype: uint8(memd.DatatypeFlagJSON)})
	c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("deleted")})
	if got := c.datatype("deleted"); got != `["raw"]` {
		t.Errorf("deleted: expected $document.datatype [\"raw\"], got %s", got)
	}
}

// TestSubdocPathOnDeletedDocument checks that a path spec on a deleted
// document fails as on a body that is not JSON: kv_engine gives a document
// it creates deleted no root, and a deleted document has no JSON datatype.
func TestSubdocPathOnDeletedDocument(t *testing.T) {
	setA := subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}

	t.Run("create", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)

		resp := c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagMkDoc|memd.SubdocDocFlagCreateAsDeleted), setMetaField, setA)
		expectSpecError(t, resp, 1, memd.StatusSubDocNotJSON)
		if status, _, _ := c.lookupDeleted("doc", memd.SubDocOpGet, memd.SubdocFlagXattrPath, "meta"); status != memd.StatusKeyNotFound {
			t.Errorf("expected the document not to be stored, got lookup status 0x%02x", uint16(status))
		}
	})

	t.Run("update", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.prepare("doc", stateDeleted)

		resp := c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagAccessDeleted), setMetaField, setA)
		expectSpecError(t, resp, 1, memd.StatusSubDocNotJSON)
		if meta := c.xattr("doc", "meta"); meta != nil {
			t.Errorf("expected the document not to be stored, got xattr meta %s", meta)
		}
	})

	t.Run("lookup", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		c.prepare("doc", stateDeleted)

		status, specStatus, _ := c.lookupDeleted("doc", memd.SubDocOpGet, 0, "a")
		if status != memd.StatusSubDocMultiPathFailureDeleted || specStatus != memd.StatusSubDocNotJSON {
			t.Errorf("expected status 0x%02x and spec status 0x%02x, got 0x%02x and 0x%02x",
				uint16(memd.StatusSubDocMultiPathFailureDeleted), uint16(memd.StatusSubDocNotJSON), uint16(status), uint16(specStatus))
		}
	})
}

// TestExpiredDocument checks that a document that expired keeps only its
// system xattrs, as kv_engine's document_pre_expiry leaves it, both for a
// lookup and for a mutation with AccessDeleted.
func TestExpiredDocument(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		t.Run(fmt.Sprintf("mutate=%t", mutate), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			c.prepare("doc", stateExpired)

			if mutate {
				expectStatus(t, "mutate", c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagAccessDeleted), setMetaField), memd.StatusSubDocSuccessDeleted)
				if meta := c.xattr("doc", "meta"); string(meta) != `{"field":"b"}` {
					t.Errorf("expected xattr meta to be stored, got %s", meta)
				}
			}
			if old := c.xattr("doc", "_old"); string(old) != `{"x":1}` {
				t.Errorf("expected system xattr _old to be kept, got %s", old)
			}
			if old := c.xattr("doc", "old"); old != nil {
				t.Errorf("expected user xattr old to be removed, got %s", old)
			}
			if size := c.xattr("doc", "$document.value_bytes"); string(size) != "0" {
				t.Errorf("expected the body to be removed, got $document.value_bytes %s", size)
			}
			if got := c.datatype("doc"); got != `["xattr"]` {
				t.Errorf("expected $document.datatype [\"xattr\"], got %s", got)
			}
		})
	}
}

// TestDeletedDocumentDoesNotExpire checks that a document created deleted
// with an expiry keeps its user xattrs once the expiry passes: kv_engine
// does not expire a deleted document (VBucket::fetchValidValue).
func TestDeletedDocumentDoesNotExpire(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)

	extras := []byte{0, 0, 0, 10, uint8(memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagCreateAsDeleted)}
	expectStatus(t, "create", c.mutateIn("doc", 0, extras, setMetaField), memd.StatusSubDocSuccessDeleted)
	c.cluster.Chrono().TimeTravel(20 * time.Second)

	if meta := c.xattr("doc", "meta"); string(meta) != `{"field":"b"}` {
		t.Errorf("expected user xattr meta to be kept, got %s", meta)
	}
	expectStatus(t, "mutate", c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagAccessDeleted), xattrSpec("meta.other", "1")), memd.StatusSubDocSuccessDeleted)
	if meta := c.xattr("doc", "meta"); string(meta) != `{"field":"b","other":1}` {
		t.Errorf("expected user xattr meta to be updated, got %s", meta)
	}
}

// TestMultiMutationCasOnDeletedDocument checks that without AccessDeleted
// the CAS is not compared with a deleted document: kv_engine refuses a
// replace of a document it does not see as missing, and stores a document it
// creates with a zero CAS.
func TestMultiMutationCasOnDeletedDocument(t *testing.T) {
	for _, test := range []struct {
		name   string
		flags  memd.SubdocDocFlag
		status memd.StatusCode
	}{
		{"replace", 0, memd.StatusKeyNotFound},
		{"mkdoc", memd.SubdocDocFlagMkDoc, memd.StatusSuccess},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			c.prepare("doc", stateDeleted)

			resp := c.mutateIn("doc", 1, docFlags(test.flags), setMetaField)
			expectStatus(t, "mutate", resp, test.status)
		})
	}
}

// TestDeletionKeepsXattrOrder checks that the system xattrs a DeleteDoc spec
// or an expiry keeps stay in the order the xattr blob held them.
func TestDeletionKeepsXattrOrder(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprintf("expire=%t", expire), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			extras := make([]byte, 8)
			var mutateExtras []byte
			if expire {
				binary.BigEndian.PutUint32(extras[4:], 10)
				mutateExtras = []byte{0, 0, 0, 10}
			}
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: extras})
			for _, key := range []string{"_b", "user", "_a"} {
				expectStatus(t, "set "+key, c.mutateIn("doc", 0, mutateExtras, xattrSpec(key+".x", "1")), memd.StatusSuccess)
			}

			if expire {
				c.cluster.Chrono().TimeTravel(20 * time.Second)
			} else {
				expectStatus(t, "delete", c.mutateIn("doc", 0, nil, subdocSpec{Op: memd.SubDocOpDeleteDoc}), memd.StatusSuccess)
			}
			if got := string(c.xattr("doc", "$XTOC")); got != `["_b","_a"]` {
				t.Errorf(`expected $XTOC ["_b","_a"], got %s`, got)
			}
		})
	}
}

// TestCreateAsDeletedOverDeletedDocumentWithCas checks that the Add of a
// deleted document over another, which fails, answers key_eexists without a
// retry to a request with a CAS, as subdoc_update does, even with the CAS of
// the deleted document, which kv_engine does not compare.
func TestCreateAsDeletedOverDeletedDocumentWithCas(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)
	c.prepare("doc", stateDeleted)
	cas, err := strconv.ParseUint(strings.Trim(string(c.xattr("doc", "$document.CAS")), `"`), 0, 64)
	if err != nil {
		t.Fatalf("failed to read the CAS: %v", err)
	}

	resp := c.mutateIn("doc", cas, docFlags(memd.SubdocDocFlagMkDoc|memd.SubdocDocFlagCreateAsDeleted), setMetaField)
	expectStatus(t, "mutate", resp, memd.StatusKeyExists)
	if meta := c.xattr("doc", "meta"); meta != nil {
		t.Errorf("expected nothing stored, got xattr meta %s", meta)
	}
}

// TestRevisionContinuesOverDeletedDocument checks that a document stored over
// a deleted one, or over one that expired, takes the next revision, as
// kv_engine's StoredValue keeps its revision sequence number.
func TestRevisionContinuesOverDeletedDocument(t *testing.T) {
	for _, state := range []deletedTestState{stateDeleted, stateExpired} {
		t.Run(state.String(), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			c.prepare("doc", state)
			before := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc")})
			revID := binary.BigEndian.Uint64(before.Extras[12:])

			c.mustSucceed(&memd.Packet{Command: memd.CmdAdd, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
			after := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc")})
			if got := binary.BigEndian.Uint64(after.Extras[12:]); got != revID+1 {
				t.Errorf("expected revision %d, got %d", revID+1, got)
			}
		})
	}
}

// TestDeleteFlagsAndExpiry checks DELETE as kv_engine's
// RemoveCommandContext serves it: a document that keeps system xattrs is
// stored deleted with no flags and no expiry (allocateDeletedItem,
// MB-25273), any other is removed with its flags and expiry. Once persisted,
// a deletion reports the time it was made as its expiry, as an EP bucket
// reads it back from disk.
func TestDeleteFlagsAndExpiry(t *testing.T) {
	for _, test := range []struct {
		name  string
		xattr string
		flags uint32
		kept  bool
	}{
		{name: "no xattrs", flags: 7, kept: true},
		{name: "user xattrs", xattr: "user.x", flags: 7, kept: true},
		{name: "system xattrs", xattr: "_sys.x", flags: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			extras := make([]byte, 8)
			binary.BigEndian.PutUint32(extras, 7)
			binary.BigEndian.PutUint32(extras[4:], 100)
			c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: extras})
			if test.xattr != "" {
				expectStatus(t, "set xattr", c.mutateIn("doc", 0, []byte{0, 0, 0, 100}, xattrSpec(test.xattr, "1")), memd.StatusSuccess)
			}
			before := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc")})
			c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte("doc")})

			after := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc")})
			if flags := binary.BigEndian.Uint32(after.Extras[4:]); flags != test.flags {
				t.Errorf("expected flags %d, got %d", test.flags, flags)
			}
			wantExptime := uint32(0)
			if test.kept {
				wantExptime = binary.BigEndian.Uint32(before.Extras[8:])
			}
			if exptime := binary.BigEndian.Uint32(after.Extras[8:]); exptime != wantExptime {
				t.Errorf("expected exptime %d, got %d", wantExptime, exptime)
			}

			c.cluster.Chrono().TimeTravel(time.Second)
			exptime, _ := strconv.ParseInt(string(c.xattr("doc", "$document.exptime")), 10, 64)
			modified, _ := strconv.ParseInt(strings.Trim(string(c.xattr("doc", "$document.last_modified")), `"`), 10, 64)
			if exptime < modified-1 || exptime > modified+1 {
				t.Errorf("expected the time of the deletion, about %d, as exptime once persisted, got %d", modified, exptime)
			}
		})
	}
}

// TestDeleteDocWithoutSystemXattrsRemoves checks that a DeleteDoc spec that
// leaves no system xattrs removes the document, as kv_engine's subdoc_update
// does before it looks at ReviveDocument: the removal keeps the flags and the
// expiry, and of a deleted document it answers key_enoent.
func TestDeleteDocWithoutSystemXattrsRemoves(t *testing.T) {
	deleteDoc := subdocSpec{Op: memd.SubDocOpDeleteDoc}

	t.Run("live", func(t *testing.T) {
		c := newTestKvConn(t, memd.FeatureXattr)
		extras := make([]byte, 8)
		binary.BigEndian.PutUint32(extras, 7)
		c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: extras})
		expectStatus(t, "delete", c.mutateIn("doc", 0, nil, xattrSpec("user.x", "1"), deleteDoc), memd.StatusSuccess)

		resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{2}})
		if deleted, flags := binary.BigEndian.Uint32(resp.Extras), binary.BigEndian.Uint32(resp.Extras[4:]); deleted != 1 || flags != 7 || resp.Extras[20] != 0 {
			t.Errorf("expected a deleted raw document with flags 7, got extras %v", resp.Extras)
		}
	})

	for _, test := range []struct {
		name  string
		flags memd.SubdocDocFlag
	}{
		{"deleted", memd.SubdocDocFlagAccessDeleted},
		{"revived", memd.SubdocDocFlagAccessDeleted | memd.SubdocDocFlagReviveDocument},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureCreateAsDeleted)
			expectStatus(t, "create", c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagAddDoc|memd.SubdocDocFlagCreateAsDeleted), xattrSpec("old.x", "1")), memd.StatusSubDocSuccessDeleted)

			expectStatus(t, "delete", c.mutateIn("doc", 0, docFlags(test.flags), xattrSpec("old.y", "2"), deleteDoc), memd.StatusKeyNotFound)
			if old := c.xattr("doc", "old"); string(old) != `{"x":1}` {
				t.Errorf("expected the document unchanged, got xattr old %s", old)
			}
		})
	}
}

// TestLocksAndTouchInMemory checks that GET_LOCKED and UNLOCK change a
// document in memory, as kv_engine's VBucket::getLocked and
// KVBucket::unlockKey do: the lock gives it a new CAS, which the unlock
// keeps, without a mutation. A TOUCH that keeps the expiry changes nothing,
// and one that changes it is a mutation with a new revision
// (VBucket::processGetAndUpdateTtl).
func TestLocksAndTouchInMemory(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	state := func() (string, string, uint64) {
		t.Helper()
		meta := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{2}})
		return string(c.xattr("doc", "$document.seqno")), string(c.xattr("doc", "$document.CAS")), binary.BigEndian.Uint64(meta.Extras[12:])
	}
	seqNo, cas, revID := state()

	locked := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{0, 0, 0, 10}})
	c.mustSucceed(&memd.Packet{Command: memd.CmdUnlockKey, Vbucket: c.vbID, Key: []byte("doc"), Cas: locked.Cas})
	lockCas := fmt.Sprintf(`"0x%016x"`, locked.Cas)
	if gotSeqNo, gotCas, gotRevID := state(); gotSeqNo != seqNo || gotCas != lockCas || gotCas == cas || gotRevID != revID {
		t.Errorf("after the lock: expected seqno %s, CAS %s, revision %d, got %s, %s, %d", seqNo, lockCas, revID, gotSeqNo, gotCas, gotRevID)
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdTouch, Vbucket: c.vbID, Key: []byte("doc"), Extras: make([]byte, 4)})
	if gotSeqNo, gotCas, gotRevID := state(); gotSeqNo != seqNo || gotCas != lockCas || gotRevID != revID {
		t.Errorf("after a touch to the same expiry: expected seqno %s, CAS %s, revision %d, got %s, %s, %d", seqNo, lockCas, revID, gotSeqNo, gotCas, gotRevID)
	}

	c.mustSucceed(&memd.Packet{Command: memd.CmdTouch, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{0, 0, 0, 100}})
	if gotSeqNo, gotCas, gotRevID := state(); gotSeqNo == seqNo || gotCas == lockCas || gotRevID != revID+1 {
		t.Errorf("after a touch to a new expiry: expected a new seqno and CAS and revision %d, got %s, %s, %d", revID+1, gotSeqNo, gotCas, gotRevID)
	}
}

// TestDocumentMacrosOfCreatedDocument checks the ${$document} macros of a
// document the request creates: kv_engine has read nothing, so they report
// an empty item_info, deleted and of raw bytes, also over a deleted document
// the request does not see.
func TestDocumentMacrosOfCreatedDocument(t *testing.T) {
	for _, state := range []deletedTestState{stateMissing, stateDeleted} {
		t.Run(state.String(), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr)
			c.prepare("doc", state)

			var specs []subdocSpec
			for _, field := range []string{"CAS", "seqno", "revid", "vbucket_uuid", "flags", "deleted", "datatype"} {
				specs = append(specs, macroSpec("m."+field, `"${$document.`+field+`}"`))
			}
			expectStatus(t, "create", c.mutateIn("doc", 0, docFlags(memd.SubdocDocFlagMkDoc), specs...), memd.StatusSuccess)

			want := map[string]string{
				"CAS": `"0x0000000000000000"`, "seqno": `"0x0000000000000000"`, "revid": `"0"`,
				"vbucket_uuid": `"0x0000000000000000"`, "flags": "0", "deleted": "true", "datatype": `["raw"]`,
			}
			for field, value := range want {
				if got := c.xattr("doc", "m."+field); string(got) != value {
					t.Errorf("expected ${$document.%s} %s, got %s", field, value, got)
				}
			}
		})
	}
}

// TestGetMeta checks GET_META as kv_engine's GetMetaCommandContext answers
// it: deleted flag, flags, expiry (0 for none), revision, and the stored
// datatype with the xattr bit only for version 2, no value, and for a
// document that expired the document as stored, reported deleted, without
// storing its deletion.
func TestGetMeta(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, 7)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: extras})
	expectStatus(t, "set xattr", c.mutateIn("doc", 0, nil, xattrSpec("x", "1")), memd.StatusSuccess)

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc")})
	if len(resp.Extras) != 20 || len(resp.Value) != 0 {
		t.Fatalf("expected 20 bytes of extras and no value, got %v and %q", resp.Extras, resp.Value)
	}
	if deleted, flags, exptime, revID := binary.BigEndian.Uint32(resp.Extras), binary.BigEndian.Uint32(resp.Extras[4:]),
		binary.BigEndian.Uint32(resp.Extras[8:]), binary.BigEndian.Uint64(resp.Extras[12:]); deleted != 0 || flags != 7 || exptime != 0 || revID != 2 {
		t.Errorf("expected live, flags 7, no expiry, revision 2, got %d %d %d %d", deleted, flags, exptime, revID)
	}
	resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{2}})
	if len(resp.Extras) != 21 || resp.Extras[20] != uint8(memd.DatatypeFlagJSON|memd.DatatypeFlagXattrs) {
		t.Errorf("expected the datatype json and xattr, got extras %v", resp.Extras)
	}

	binary.BigEndian.PutUint32(extras[4:], 10)
	set := c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("expired"), Value: []byte(`{"a":1}`), Extras: extras})
	c.cluster.Chrono().TimeTravel(20 * time.Second)
	for i := 0; i < 2; i++ {
		resp = c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte("expired"), Extras: []byte{2}})
		if resp.Cas != set.Cas || binary.BigEndian.Uint32(resp.Extras) != 1 || binary.BigEndian.Uint64(resp.Extras[12:]) != 1 || resp.Extras[20] != uint8(memd.DatatypeFlagJSON) {
			t.Errorf("expected the expired document as stored, reported deleted, got CAS 0x%x extras %v", resp.Cas, resp.Extras)
		}
	}
}

// TestMultiLookupComparesCas checks that a lookup with a CAS reads only the
// document with that CAS, and a locked document with any CAS but the one a
// GET reports for it (SubdocCmdContext::get_document_for_searching).
func TestMultiLookupComparesCas(t *testing.T) {
	c := newTestKvConn(t)
	cas := c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{"a":1}`), Extras: make([]byte, 8)}).Cas
	lookup := func(cas uint64) memd.StatusCode {
		return c.Do(&memd.Packet{Command: memd.CmdSubDocMultiLookup, Vbucket: c.vbID, Key: []byte("doc"), Cas: cas, Value: encodeLookupSpec(memd.SubDocOpGet, "a")}).Status
	}

	if status := lookup(cas); status != memd.StatusSuccess {
		t.Errorf("lookup with the CAS: expected success, got 0x%02x", uint16(status))
	}
	if status := lookup(cas + 1); status != memd.StatusKeyExists {
		t.Errorf("lookup with another CAS: expected status 0x%02x, got 0x%02x", uint16(memd.StatusKeyExists), uint16(status))
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("doc")})
	if status := lookup(0xffffffffffffffff); status != memd.StatusLocked {
		t.Errorf("lookup of a locked document with the locked CAS: expected status 0x%02x, got 0x%02x", uint16(memd.StatusLocked), uint16(status))
	}
	if status := lookup(cas + 1); status != memd.StatusSuccess {
		t.Errorf("lookup of a locked document with another CAS: expected success, got 0x%02x", uint16(status))
	}
}

// TestLockedDocumentAsRead checks what a lookup of a locked document with a
// CAS sees and answers: $document reports the CAS a read reports for a
// locked document, 0xffffffffffffffff, and last_modified from it, and the
// response carries the CAS of the request (get_document_for_searching).
// GET_RANDOM reports the CAS the lock gave, and UNLOCK answers with no CAS.
func TestLockedDocumentAsRead(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr)
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte(`{}`), Extras: make([]byte, 8)})
	c.cluster.Chrono().TimeTravel(time.Second)
	locked := c.mustSucceed(&memd.Packet{Command: memd.CmdGetLocked, Vbucket: c.vbID, Key: []byte("doc"), Extras: []byte{0, 0, 0, 10}})

	spec := encodeLookupSpec(memd.SubDocOpGet, "$document")
	spec[1] = uint8(memd.SubdocFlagXattrPath)
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiLookup, Vbucket: c.vbID, Key: []byte("doc"), Cas: locked.Cas + 1, Value: spec})
	if resp.Cas != locked.Cas+1 {
		t.Errorf("expected the response CAS %x, got %x", locked.Cas+1, resp.Cas)
	}
	var vattr struct {
		CAS          string `json:"CAS"`
		LastModified string `json:"last_modified"`
	}
	if err := json.Unmarshal(resp.Value[6:], &vattr); err != nil || vattr.CAS != "0xffffffffffffffff" || vattr.LastModified != "0" {
		t.Errorf("expected CAS 0xffffffffffffffff and last_modified 0, got %s (%v)", resp.Value[6:], err)
	}

	random := c.mustSucceed(&memd.Packet{Command: memd.CmdGetRandom, Vbucket: c.vbID})
	if random.Cas != locked.Cas {
		t.Errorf("GET_RANDOM: expected the CAS of the lock %x, got %x", locked.Cas, random.Cas)
	}

	unlock := c.mustSucceed(&memd.Packet{Command: memd.CmdUnlockKey, Vbucket: c.vbID, Key: []byte("doc"), Cas: locked.Cas})
	if unlock.Cas != 0 || len(unlock.Extras) != 0 {
		t.Errorf("UNLOCK: expected no CAS and no extras, got CAS %x extras %v", unlock.Cas, unlock.Extras)
	}
}

// TestPreserveExpiryOfDocumentRead checks that PreserveTtl keeps the expiry of
// the document read, a deleted one too: an expired document keeps the expiry
// it had, and a persisted deletion the time it was made, which it reports as
// its expiry. A document revived with an expiry that has passed expires again
// (subdoc_executor, fetchedItem).
func TestPreserveExpiryOfDocumentRead(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureAltRequests, memd.FeaturePreserveExpiry)
	exptime := func(key string) uint32 {
		t.Helper()
		resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGetMeta, Vbucket: c.vbID, Key: []byte(key)})
		return binary.BigEndian.Uint32(resp.Extras[8:])
	}
	withExpiry := make([]byte, 8)
	binary.BigEndian.PutUint32(withExpiry[4:], 10)

	for _, test := range []struct {
		name   string
		revive bool
		expire bool
	}{
		{name: "expired", expire: true},
		{name: "expired, revived", expire: true, revive: true},
		{name: "persisted deletion"},
		{name: "persisted deletion, revived", revive: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := test.name
			if test.expire {
				c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte(key), Value: []byte(`{}`), Extras: withExpiry})
				c.mustSucceed(&memd.Packet{Command: memd.CmdSubDocMultiMutation, Vbucket: c.vbID, Key: []byte(key),
					Value: encodeMutationSpecs(xattrSpec("_s", "1")), PreserveExpiryFrame: &memd.PreserveExpiryFrame{}})
				c.cluster.Chrono().TimeTravel(20 * time.Second)
			} else {
				c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte(key), Value: []byte(`{}`), Extras: make([]byte, 8)})
				expectStatus(t, "set xattr", c.mutateIn(key, 0, nil, xattrSpec("_s", "1")), memd.StatusSuccess)
				c.mustSucceed(&memd.Packet{Command: memd.CmdDelete, Vbucket: c.vbID, Key: []byte(key)})
				c.cluster.Chrono().TimeTravel(time.Second)
			}
			before := exptime(key)
			if before == 0 {
				t.Fatalf("expected the deleted document to report an expiry")
			}

			flags := memd.SubdocDocFlagAccessDeleted
			status := memd.StatusSubDocSuccessDeleted
			if test.revive {
				flags |= memd.SubdocDocFlagReviveDocument
				status = memd.StatusSuccess
			}
			resp := c.Do(&memd.Packet{
				Command:             memd.CmdSubDocMultiMutation,
				Vbucket:             c.vbID,
				Key:                 []byte(key),
				Extras:              docFlags(flags),
				Value:               encodeMutationSpecs(xattrSpec("_s", "2")),
				PreserveExpiryFrame: &memd.PreserveExpiryFrame{},
			})
			expectStatus(t, "mutation", resp, status)
			if after := exptime(key); after != before {
				t.Errorf("expected the expiry %d kept, got %d", before, after)
			}
			if get := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte(key)}); get.Status != memd.StatusKeyNotFound {
				t.Errorf("GET: expected status 0x%02x, got 0x%02x", uint16(memd.StatusKeyNotFound), uint16(get.Status))
			}
		})
	}
}
