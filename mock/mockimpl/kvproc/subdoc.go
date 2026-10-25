package kvproc

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"hash/crc32"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

var (
	crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")
	seqnoMacro  = []byte("\"${Mutation.seqno}\"")
	casMacro    = []byte("\"${Mutation.CAS}\"")

	// crc32cPlaceholder holds the place of ${Mutation.value_crc32c} until the
	// body is final. kv_engine pads macros with random bytes alike.
	crc32cPlaceholder = macroPlaceholder("${Mutation.value_crc32c}")
	// SeqNoPlaceholder holds the place of ${Mutation.seqno}, which the store
	// replaces once the mutation has its sequence number.
	SeqNoPlaceholder = macroPlaceholder("${Mutation.seqno}")
)

func macroPlaceholder(macro string) []byte {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return []byte(`"` + macro + hex.EncodeToString(b) + `"`)
}

// executeSdOps runs the specs on doc. created says doc is one the request
// creates.
func (e *Engine) executeSdOps(doc, newMeta *mockdb.Document, ops []*SubDocOp, continueOnOpError, created bool) ([]*SubDocResult, error) {
	opReses := make([]*SubDocResult, len(ops))
	expandedMacros := false

	// $document describes the document as read, before any spec runs
	// (SubdocCmdContext::get_document_vattr). For a document the request
	// creates, kv_engine has read nothing: its item_info is empty and
	// reports a deleted document of raw bytes.
	read := *doc
	read.Value = bytes.Clone(doc.Value)
	read.Xattrs = maps.Clone(doc.Xattrs)
	read.XattrOrder = slices.Clone(doc.XattrOrder)
	if created {
		read = mockdb.Document{Value: read.Value, IsDeleted: true}
	}

	// kv_engine runs the xattr specs, then the body specs; the validator
	// keeps the xattr specs first, so that is the order of the request.
	var xattrs xattrPhase
	// kv_engine runs a body spec only on a document whose datatype is JSON.
	// The specs keep the datatype of the document they read, except that a
	// whole-document spec decides it again from the text it leaves
	// (operate_single_doc).
	for opIdx, op := range ops {
		var opDoc *mockdb.Document
		if op.IsXattrPath {
			// A macro is expanded into a copy, so a retry expands it again.
			if op.ExpandMacros {
				expanded := *op
				op = &expanded
				expandedMacros = true
			}
			var err error
			opDoc, err = e.createXattrDoc(doc, &read, newMeta, op, &xattrs)
			if err != nil && !continueOnOpError {
				return nil, SubdocMutateError{Index: opIdx, Err: err}
			} else if err != nil {
				opReses[opIdx] = &SubDocResult{
					Value: nil,
					Err:   err,
				}
				continue
			}
		} else {
			opDoc = doc
		}
		base := baseSubDocExecutor{
			doc:     opDoc,
			notJSON: !op.IsXattrPath && doc.Datatype&uint8(memd.DatatypeFlagJSON) == 0,
		}

		var executor SubDocExecutor
		switch op.Op {
		case memd.SubDocOpGet:
			executor = SubDocGetExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpExists:
			executor = SubDocExistsExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpGetCount:
			executor = SubDocGetCountExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpGetDoc:
			executor = SubDocGetDocExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDictAdd:
			executor = SubDocDictAddExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDictSet:
			executor = SubDocDictSetExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDelete:
			executor = SubDocDeleteExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpReplace:
			executor = SubDocReplaceExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayPushLast:
			executor = SubDocArrayPushLastExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayPushFirst:
			executor = SubDocArrayPushFirstExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayInsert:
			executor = SubDocArrayInsertExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayAddUnique:
			executor = SubDocArrayAddUniqueExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpCounter:
			executor = SubDocCounterExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpSetDoc:
			executor = SubDocDictSetFullExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDeleteDoc:
			executor = SubDocDeleteFullDocExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpReplaceBodyWithXattr:
			executor = SubDocGetExecutor{
				baseSubDocExecutor: base,
			}
		}

		if executor == nil {
			return nil, ErrInternal
		}

		opRes, err := executor.Execute(op)
		if err != nil {
			return nil, err
		}

		if !continueOnOpError && opRes.Err != nil {
			return nil, SubdocMutateError{Index: opIdx, Err: opRes.Err}
		}

		if opRes == nil {
			return nil, ErrInternal
		}

		switch {
		case (op.Op == memd.SubDocOpSetDoc || op.Op == memd.SubDocOpDeleteDoc) && opRes.Err == nil:
			doc.Datatype = SetDatatypeJSONFromValue(doc.Datatype, doc.Value)
		case op.Op == memd.SubDocOpReplaceBodyWithXattr && opRes.Err == nil:
			// The value of the xattr path becomes the body, which is JSON
			// unless empty, and the spec returns nothing
			// (subdoc_operate_attributes_and_body).
			doc.Value = opRes.Value
			if len(doc.Value) > 0 {
				doc.Datatype |= uint8(memd.DatatypeFlagJSON)
			} else {
				doc.Datatype &^= uint8(memd.DatatypeFlagJSON)
			}
			opRes.Value = nil
		}

		if op.IsXattrPath && opRes.Err == nil && subdocOpIsMutation(op) {
			xattrs.modified = true
		}

		opReses[opIdx] = opRes
	}

	if xattrs.modified {
		if err := xattrs.store(doc); err != nil {
			return nil, err
		}
	}

	// kv_engine expands ${Mutation.value_crc32c} once the body is final
	// (SubdocCmdContext::pre_link_document), in place of padding of the
	// same size, so the xattr keeps its position.
	if expandedMacros {
		crc := []byte(fmt.Sprintf(`"0x%08x"`, crc32.Checksum(doc.Value, crc32.MakeTable(crc32.Castagnoli))))
		for key, value := range doc.Xattrs {
			doc.Xattrs[key] = bytes.ReplaceAll(value, crc32cPlaceholder, crc)
		}
	}

	return opReses, nil
}

// xattrPhase is the document the xattr specs of a request run on. As
// kv_engine's do_xattr_phase (daemon/subdocument.cc@v7.0.5) does, it holds
// {"<key>":<value>} for the request's xattr key, or {} if the document has no
// such xattr, every xattr spec runs on it in turn, and the result is stored
// once.
type xattrPhase struct {
	key      string
	doc      *mockdb.Document
	modified bool
}

// store stores in doc the xattr the specs left: the text after the first
// key, or nothing if the specs removed every key.
func (x *xattrPhase) store(doc *mockdb.Document) error {
	text := x.doc.Value
	key := 1
	for key < len(text) && isJSONWhitespace(text[key]) {
		key++
	}
	if key >= len(text) || text[key] != '"' {
		doc.RemoveXattr(x.key)
		return nil
	}
	end := key + 1
	for end < len(text) && text[end] != '"' {
		if text[end] == '\\' {
			end++
		}
		end++
	}
	colon := end + bytes.IndexByte(text[end:], ':')
	doc.SetXattr(x.key, append([]byte{}, text[colon+1:len(text)-1]...))
	return nil
}

// xattrKey returns the xattr key of an xattr spec's path: what precedes the
// first '.' or '[', as in kv_engine's is_valid_xattr_key. The rest of the path
// is parsed when the spec runs.
func xattrKey(path string) string {
	if end := strings.IndexAny(path, ".["); end >= 0 {
		return path[:end]
	}
	return path
}

// createXtocDoc returns the $XTOC virtual attribute kv_engine's
// get_xtoc_vattr (daemon/subdocument_context.cc@v7.0.5) builds: the keys of
// the document's xattrs, in the order its xattr blob holds them.
func createXtocDoc(doc *mockdb.Document) (*mockdb.Document, error) {
	value, err := marshalJSON(map[string][]string{"$XTOC": doc.XattrKeys()})
	if err != nil {
		return nil, err
	}
	return &mockdb.Document{Value: value}, nil
}

func (e *Engine) createXattrDoc(doc, read, metaDoc *mockdb.Document, op *SubDocOp, xattrs *xattrPhase) (*mockdb.Document, error) {
	if op.ExpandMacros {
		var err error
		op.Value, err = e.createMacroValue(op.Value, read, metaDoc)
		if err != nil {
			return nil, err
		}
	}

	key := xattrKey(op.Path)
	switch key {
	case "$document":
		return e.createVattrDoc(read), nil
	case "$vbucket":
		return e.createVbucketDoc(), nil
	case "$XTOC":
		return createXtocDoc(doc)
	}
	if strings.HasPrefix(key, "$") {
		return nil, ErrSdUnknownVattr
	}

	if xattrs.doc == nil {
		xattrs.key = key
		xattrs.doc = &mockdb.Document{Value: []byte("{}")}
		if xattr, ok := doc.Xattrs[key]; ok {
			value := append([]byte(`{"`+key+`":`), xattr...)
			xattrs.doc.Value = append(value, '}')
		}
	}
	return xattrs.doc, nil
}

func (e *Engine) createMacroValue(opValue []byte, doc, metaDoc *mockdb.Document) ([]byte, error) {
	var val []byte
	if bytes.Equal(opValue, casMacro) {
		binaryEncodedCas := make([]byte, 8)
		binary.LittleEndian.PutUint64(binaryEncodedCas, metaDoc.Cas)
		val = []byte(fmt.Sprintf("\"0x%v\"", hex.EncodeToString(binaryEncodedCas)))
	} else if bytes.Equal(opValue, crc32cMacro) {
		val = crc32cPlaceholder
	} else if bytes.Equal(opValue, seqnoMacro) {
		val = SeqNoPlaceholder
	} else if bytes.HasPrefix(opValue, []byte(`"${$document`)) {
		var ok bool
		if val, ok = documentMacro(string(opValue), doc); !ok {
			return nil, ErrSdUnknownVattrMacro
		}
	} else {
		return nil, ErrUnknownXattrMacro
	}

	return val, nil
}

// documentMacro expands a ${$document} macro from doc, the document as read,
// as kv_engine's expand_virtual_document_macro
// (daemon/subdocument_context.cc@v7.0.5) does, and reports whether it is
// one.
func documentMacro(macro string, doc *mockdb.Document) ([]byte, bool) {
	var v string
	switch macro {
	case `"${$document}"`:
		return documentVattr(doc), true
	case `"${$document.CAS}"`:
		v = fmt.Sprintf(`"0x%016x"`, doc.Cas)
	case `"${$document.vbucket_uuid}"`:
		v = fmt.Sprintf(`"0x%016x"`, doc.VbUUID)
	case `"${$document.seqno}"`:
		v = fmt.Sprintf(`"0x%016x"`, doc.SeqNo)
	case `"${$document.revid}"`:
		v = fmt.Sprintf(`"%d"`, doc.RevID)
	case `"${$document.exptime}"`:
		v = strconv.FormatInt(documentExptime(doc), 10)
	case `"${$document.flags}"`:
		v = strconv.FormatUint(uint64(doc.Flags), 10)
	case `"${$document.value_bytes}"`:
		v = strconv.Itoa(len(doc.Value))
	case `"${$document.datatype}"`:
		v = "[" + strings.Join(datatypeToString(doc.Datatype, len(doc.Xattrs) > 0), ",") + "]"
	case `"${$document.deleted}"`:
		v = strconv.FormatBool(doc.IsDeleted)
	case `"${$document.last_modified}"`:
		if !doc.CasIsHLC {
			return nil, false
		}
		v = strconv.FormatInt(int64(doc.Cas)/int64(time.Second), 10)
	default:
		return nil, false
	}
	return []byte(v), true
}

func documentExptime(doc *mockdb.Document) int64 {
	if doc.Expiry.IsZero() {
		return 0
	}
	return doc.Expiry.Unix()
}

// documentVattr returns the fields of the $document virtual attribute, as
// kv_engine's get_document_vattr builds them, in its key order.
func documentVattr(doc *mockdb.Document) []byte {
	v := []byte{'{'}
	v = append(v, fmt.Sprintf("\"CAS\":\"0x%016x\",", doc.Cas)...)
	v = append(v, fmt.Sprintf("\"datatype\":[%s],", strings.Join(datatypeToString(doc.Datatype, len(doc.Xattrs) > 0), ","))...)
	v = append(v, fmt.Sprintf("\"deleted\":%t,", doc.IsDeleted)...)
	v = append(v, fmt.Sprintf("\"exptime\":%d,", documentExptime(doc))...)
	v = append(v, fmt.Sprintf("\"flags\":%d,", doc.Flags)...)
	// last_modified is the CAS in seconds, when it is a hybrid logical
	// clock.
	if doc.CasIsHLC {
		v = append(v, fmt.Sprintf("\"last_modified\":\"%d\",", int64(doc.Cas)/int64(time.Second))...)
	}
	v = append(v, fmt.Sprintf("\"revid\":\"%d\",", doc.RevID)...)
	v = append(v, fmt.Sprintf("\"seqno\":\"0x%016x\",", doc.SeqNo)...)
	v = append(v, fmt.Sprintf("\"value_bytes\":%d,", len(doc.Value))...)
	v = append(v, fmt.Sprintf("\"value_crc32c\":\"0x%08x\",", crc32.Checksum(doc.Value, crc32.MakeTable(crc32.Castagnoli)))...)
	v = append(v, fmt.Sprintf("\"vbucket_uuid\":\"0x%016x\"", doc.VbUUID)...)
	return append(v, '}')
}

func (e *Engine) createVbucketDoc() *mockdb.Document {
	v := []byte(fmt.Sprintf(`{"$vbucket":{"HLC":{"mode":"real","now":"%d"}}}`, e.HLC().Unix()))

	return &mockdb.Document{
		Value: v,
	}
}

func (e *Engine) createVattrDoc(doc *mockdb.Document) *mockdb.Document {
	v := []byte(`{"$document":`)
	v = append(v, documentVattr(doc)...)
	return &mockdb.Document{Value: append(v, '}')}
}

// datatypeToString lists the datatype of a document as kv_engine's
// mcbp::datatype::to_string does. mockdb keeps the xattrs apart from the
// body, so their datatype bit is not in datatype.
func datatypeToString(datatype uint8, hasXattrs bool) []string {
	var typ []string
	if datatype&uint8(memd.DatatypeFlagCompressed) != 0 {
		typ = append(typ, "\"snappy\"")
	}
	if datatype&uint8(memd.DatatypeFlagJSON) != 0 {
		typ = append(typ, "\"json\"")
	}
	if hasXattrs {
		typ = append(typ, "\"xattr\"")
	}
	if typ == nil {
		return []string{"\"raw\""}
	}
	return typ
}

// subdocOpIsSubJSON reports whether a spec operates on JSON with subjson,
// kv_engine's CommandScope::SubJSON, rather than on the whole document.
func subdocOpIsSubJSON(op *SubDocOp) bool {
	switch op.Op {
	case memd.SubDocOpGetDoc, memd.SubDocOpSetDoc, memd.SubDocOpDeleteDoc, memd.SubDocOpReplaceBodyWithXattr:
		return false
	}
	return true
}

// subdocRootTemplate is the body kv_engine creates a document with for a
// spec, from subjson's Util::get_root_type: an array for a path starting
// with an index or an empty path of an array spec, an object for any other
// path, and none for an empty path of any other spec.
func subdocRootTemplate(op *SubDocOp) []byte {
	path := strings.TrimLeft(op.Path, " \t\n\v\f\r")
	switch {
	case path == "":
		switch op.Op {
		case memd.SubDocOpArrayPushLast, memd.SubDocOpArrayPushFirst, memd.SubDocOpArrayAddUnique:
			return []byte("[]")
		}
		return nil
	case path[0] == '[':
		return []byte("[]")
	}
	return []byte("{}")
}

func subdocOpIsMutation(op *SubDocOp) bool {
	switch op.Op {
	case memd.SubDocOpDictAdd:
		return true
	case memd.SubDocOpDictSet:
		return true
	case memd.SubDocOpDelete:
		return true
	case memd.SubDocOpReplace:
		return true
	case memd.SubDocOpArrayPushLast:
		return true
	case memd.SubDocOpArrayPushFirst:
		return true
	case memd.SubDocOpArrayInsert:
		return true
	case memd.SubDocOpArrayAddUnique:
		return true
	case memd.SubDocOpCounter:
		return true
	case memd.SubDocOpSetDoc:
		return true
	case memd.SubDocOpDeleteDoc:
		return true
	default:
		return false
	}
}

// SubDocExecutor is an executor for subdocument operations.
type SubDocExecutor interface {
	Execute(op *SubDocOp) (*SubDocResult, error)
}

type baseSubDocExecutor struct {
	doc *mockdb.Document
	// notJSON is set for a body that is not JSON.
	notJSON bool
}

func (e baseSubDocExecutor) itemErrorResult(err error) (*SubDocResult, error) {
	return &SubDocResult{
		Value: nil,
		Err:   err,
	}, nil
}

// operation parses the path of op and returns the subjson operation on the
// document the executor runs on. A body that is not JSON fails first, as
// kv_engine checks the document's datatype before it runs a spec.
func (e baseSubDocExecutor) operation(op *SubDocOp) (*subdocOperation, error) {
	if e.notJSON {
		return nil, ErrSdNotJSON
	}
	comps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return nil, err
	}
	return newSubdocOperation(e.doc.Value, comps, op.Value, op.CreatePath), nil
}

// lookup runs a lookup operation and returns its result.
func (e baseSubDocExecutor) lookup(op *SubDocOp, run func(o *subdocOperation) ([]byte, error)) (*SubDocResult, error) {
	o, err := e.operation(op)
	if err != nil {
		return e.itemErrorResult(err)
	}
	value, err := run(o)
	if err != nil {
		return e.itemErrorResult(err)
	}
	return &SubDocResult{Value: append([]byte{}, value...)}, nil
}

// mutate runs a mutation and stores the document it returns.
func (e baseSubDocExecutor) mutate(op *SubDocOp, run func(o *subdocOperation) ([]byte, error)) (*SubDocResult, error) {
	o, err := e.operation(op)
	if err != nil {
		return e.itemErrorResult(err)
	}
	doc, err := run(o)
	if err != nil {
		return e.itemErrorResult(err)
	}
	e.doc.Value = doc
	return &SubDocResult{}, nil
}

// SubDocGetExecutor is an executor for subdocument get operations.
type SubDocGetExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.lookup(op, (*subdocOperation).get)
}

// SubDocExistsExecutor is an executor for subdocument operations.
type SubDocExistsExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocExistsExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	res, err := e.lookup(op, (*subdocOperation).get)
	if res != nil {
		res.Value = nil
	}
	return res, err
}

// SubDocGetCountExecutor is an executor for subdocument operations.
type SubDocGetCountExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetCountExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.lookup(op, (*subdocOperation).containerSize)
}

// SubDocGetDocExecutor is an executor for subdocument operations.
type SubDocGetDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetDocExecutor) Execute(_ *SubDocOp) (*SubDocResult, error) {
	// TODO: check what happens with a full doc against an xattr
	if e.doc.IsDeleted {
		return &SubDocResult{
			Value: nil,
			Err:   ErrSdPathNotFound,
		}, nil
	}
	return &SubDocResult{
		Value: e.doc.Value,
		Err:   nil,
	}, nil
}

// SubDocDictSetExecutor is an executor for subdocument operations.
type SubDocDictSetExecutor struct {
	baseSubDocExecutor
}

// Execute runs this SubDocDictSetExecutor
func (e SubDocDictSetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) { return o.storeDict(dictUpsert) })
}

// SubDocDictAddExecutor is an executor for subdocument operations.
type SubDocDictAddExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDictAddExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) { return o.storeDict(dictAdd) })
}

// SubDocReplaceExecutor is an executor for subdocument operations.
type SubDocReplaceExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocReplaceExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) {
		if err := noCreatePath(op); err != nil {
			return nil, err
		}
		return o.storeDict(dictReplace)
	})
}

// noCreatePath fails replace and array insert with create path, which only
// Mkdoc and Add imply for them: subjson has no create-path form of either and
// answers GLOBAL_ENOSUPPORT once the path has parsed, which kv_engine sends
// as an internal error (subdocument.cc@v7.0.5).
func noCreatePath(op *SubDocOp) error {
	if op.CreatePath {
		return ErrInternal
	}
	return nil
}

// SubDocDeleteExecutor is an executor for subdocument operations.
type SubDocDeleteExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, (*subdocOperation).remove)
}

// SubDocCounterExecutor is an executor for subdocument operations.
type SubDocCounterExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocCounterExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	o, err := e.operation(op)
	if err != nil {
		return e.itemErrorResult(err)
	}
	doc, value, err := o.counter()
	if err != nil {
		return e.itemErrorResult(err)
	}
	e.doc.Value = doc
	return &SubDocResult{Value: value}, nil
}

// SubDocArrayPushFirstExecutor is an executor for subdocument operations.
type SubDocArrayPushFirstExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushFirstExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, (*subdocOperation).prepend)
}

// SubDocArrayPushLastExecutor is an executor for subdocument operations.
type SubDocArrayPushLastExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushLastExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) { return o.appendValue(false) })
}

// SubDocArrayInsertExecutor is an executor for subdocument operations.
type SubDocArrayInsertExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayInsertExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) {
		if err := noCreatePath(op); err != nil {
			return nil, err
		}
		return o.insert()
	})
}

// SubDocArrayAddUniqueExecutor is an executor for subdocument operations.
type SubDocArrayAddUniqueExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayAddUniqueExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	return e.mutate(op, func(o *subdocOperation) ([]byte, error) { return o.appendValue(true) })
}

// SubDocDeleteFullDocExecutor is an executor for subdocument operations.
type SubDocDeleteFullDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteFullDocExecutor) Execute(_ *SubDocOp) (*SubDocResult, error) {
	e.doc.IsDeleted = true
	e.doc.Value = nil

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDictSetFullExecutor is an executor for subdocument operations.
type SubDocDictSetFullExecutor struct {
	baseSubDocExecutor
}

// Execute runs this SubDocDicSetFullExecutor
func (e SubDocDictSetFullExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	e.doc.Value = op.Value

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}
