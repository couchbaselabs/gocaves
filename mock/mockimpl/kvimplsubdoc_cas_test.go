package mockimpl

import (
	"fmt"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

// TestMultiMutationCasOfCreatedDocument sends a CAS with Mkdoc for a missing
// key or a deleted document, and with AccessDeleted for a deleted document.
// kv_engine compares the CAS only with a document it read
// (get_document_for_searching), whatever the specs, and stores one it creates
// with Add semantics and CAS 0 (subdoc_update).
func TestMultiMutationCasOfCreatedDocument(t *testing.T) {
	bodySpec := subdocSpec{Op: memd.SubDocOpDictSet, Path: "a", Value: "1"}

	for _, test := range []struct {
		flags  memd.SubdocDocFlag
		state  deletedTestState
		spec   subdocSpec
		status memd.StatusCode
		// body is the body a GET returns afterwards, or "" for none.
		body string
	}{
		{memd.SubdocDocFlagMkDoc, stateMissing, bodySpec, memd.StatusSuccess, `{"a":1}`},
		{memd.SubdocDocFlagMkDoc, stateMissing, setMetaField, memd.StatusSuccess, `{}`},
		{memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAccessDeleted, stateMissing, bodySpec, memd.StatusSuccess, `{"a":1}`},
		{memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAccessDeleted, stateMissing, setMetaField, memd.StatusSuccess, `{}`},
		{memd.SubdocDocFlagMkDoc, stateDeleted, bodySpec, memd.StatusSuccess, `{"a":1}`},
		{memd.SubdocDocFlagMkDoc, stateDeleted, setMetaField, memd.StatusSuccess, `{}`},
		{memd.SubdocDocFlagAccessDeleted, stateDeleted, bodySpec, memd.StatusKeyExists, ""},
		{memd.SubdocDocFlagAccessDeleted, stateDeleted, setMetaField, memd.StatusKeyExists, ""},
		{memd.SubdocDocFlagMkDoc | memd.SubdocDocFlagAccessDeleted, stateDeleted, bodySpec, memd.StatusKeyExists, ""},
	} {
		specName := "body"
		if test.spec.Flags&memd.SubdocFlagXattrPath != 0 {
			specName = "xattr"
		}
		t.Run(fmt.Sprintf("%s/0x%02x/%s", test.state, uint8(test.flags), specName), func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureXattr, memd.FeatureJSON)
			c.prepare("doc", test.state)

			expectStatus(t, "mutate", c.mutateIn("doc", 12345, docFlags(test.flags), test.spec), test.status)

			resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("doc")})
			if test.body == "" {
				expectStatus(t, "get", resp, memd.StatusKeyNotFound)
			} else {
				expectStatus(t, "get", resp, memd.StatusSuccess)
				if string(resp.Value) != test.body {
					t.Errorf("expected body %s, got %s", test.body, resp.Value)
				}
			}
			if test.spec == setMetaField {
				meta := c.xattr("doc", "meta")
				if test.status == memd.StatusSuccess && string(meta) != `{"field":"b"}` {
					t.Errorf(`expected xattr meta {"field":"b"}, got %s`, meta)
				}
				if test.status != memd.StatusSuccess && meta != nil {
					t.Errorf("expected no xattr meta, got %s", meta)
				}
			}
			if test.state == stateDeleted && test.status != memd.StatusSuccess {
				if old := c.xattr("doc", "_old"); string(old) != `{"x":1}` {
					t.Errorf(`expected the deleted document unchanged with xattr _old {"x":1}, got %s`, old)
				}
			}
		})
	}
}
