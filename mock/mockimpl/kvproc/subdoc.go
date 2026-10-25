package kvproc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"hash/crc32"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

var (
	crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")
	seqnoMacro  = []byte("\"${Mutation.seqno}\"")
	casMacro    = []byte("\"${Mutation.CAS}\"")
)

type subdocOpList struct {
	ops     []*SubDocOp
	indexes []int
}

func subdocReorder(ops []*SubDocOp) *subdocOpList {
	var xAttrOps []*SubDocOp
	var xAttrIndexes []int
	var sops []*SubDocOp
	var opIndexes []int
	for i, op := range ops {
		if op.IsXattrPath {
			xAttrOps = append(xAttrOps, op)
			xAttrIndexes = append(xAttrIndexes, i)
		} else {
			sops = append(sops, op)
			opIndexes = append(opIndexes, i)
		}
	}

	return &subdocOpList{
		ops:     append(sops, xAttrOps...),
		indexes: append(opIndexes, xAttrIndexes...),
	}
}

func (e *Engine) executeSdOps(doc, newMeta *mockdb.Document, ops []*SubDocOp, continueOnOpError bool) ([]*SubDocResult, error) {
	opReses := make([]*SubDocResult, len(ops))

	reorderedOps := subdocReorder(ops)

	var xattrs xattrPhase
	// kv_engine runs a body spec only on a document whose datatype is JSON.
	// The specs keep the datatype of the document they read, except that a
	// whole-document set decides it again from the text it stores
	// (subdocument.cc@v7.0.5).
	for opIdx, op := range reorderedOps.ops {
		var opDoc *mockdb.Document
		if op.IsXattrPath {
			var err error
			opDoc, err = e.createXattrDoc(doc, newMeta, op, &xattrs)
			if err != nil && !continueOnOpError {
				return nil, SubdocMutateError{Index: reorderedOps.indexes[opIdx], Err: err}
			} else if err != nil {
				opReses[reorderedOps.indexes[opIdx]] = &SubDocResult{
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
		}

		if executor == nil {
			return nil, ErrInternal
		}

		opRes, err := executor.Execute(op)
		if err != nil {
			return nil, err
		}

		if !continueOnOpError && opRes.Err != nil {
			return nil, SubdocMutateError{Index: reorderedOps.indexes[opIdx], Err: opRes.Err}
		}

		if opRes == nil {
			return nil, ErrInternal
		}

		if op.Op == memd.SubDocOpSetDoc && opRes.Err == nil {
			doc.Datatype = SetDatatypeJSONFromValue(doc.Datatype, doc.Value)
		}

		if op.IsXattrPath && opRes.Err == nil && subdocOpIsMutation(op) {
			xattrs.modified = true
		}

		opReses[reorderedOps.indexes[opIdx]] = opRes
	}

	if xattrs.modified {
		if err := xattrs.store(doc); err != nil {
			return nil, err
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

func (e *Engine) createXattrDoc(doc, metaDoc *mockdb.Document, op *SubDocOp, xattrs *xattrPhase) (*mockdb.Document, error) {
	if op.ExpandMacros {
		var err error
		op.Value, err = e.createMacroValue(op.Value, doc, metaDoc)
		if err != nil {
			return nil, err
		}
	}

	key := xattrKey(op.Path)
	switch key {
	case "$document":
		return e.createVattrDoc(doc), nil
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

const (
	// DeleteCrc32c CRC-32 checksum represents the body hash of "Deleted" document.
	DeleteCrc32c = "0x00000000"
)

func (e *Engine) createMacroValue(opValue []byte, doc, metaDoc *mockdb.Document) ([]byte, error) {
	var val []byte
	if bytes.Equal(opValue, casMacro) {
		binaryEncodedCas := make([]byte, 8)
		binary.LittleEndian.PutUint64(binaryEncodedCas, metaDoc.Cas)
		val = []byte(fmt.Sprintf("\"0x%v\"", hex.EncodeToString(binaryEncodedCas)))
	} else if bytes.Equal(opValue, crc32cMacro) {
		if doc.IsDeleted {
			val = []byte(fmt.Sprintf("\"%s\"", DeleteCrc32c))
		} else {
			table := crc32.MakeTable(crc32.Castagnoli)
			val = []byte(fmt.Sprintf("\"0x%x\"", crc32.Checksum(doc.Value, table)))
		}
	} else if bytes.Equal(opValue, seqnoMacro) {
		return nil, ErrUnknownXattrMacro
	} else if bytes.HasPrefix(opValue, []byte(`"${$document`)) && bytes.HasSuffix(opValue, []byte(`}"`)) {
		vattr := e.createVattrDoc(doc)
		if bytes.Equal(opValue, []byte(`"${$document}"`)) {
			// All of the vattrs!
			val = vattr.Value
		} else {
			key := bytes.TrimPrefix(opValue, []byte(`"${$document.`))
			key = bytes.TrimSuffix(key, []byte(`}"`))
			var vattrMap map[string]json.RawMessage
			err := json.Unmarshal(vattr.Value, &vattrMap)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(vattrMap["$document"], &vattrMap)
			if err != nil {
				return nil, err
			}

			var ok bool
			val, ok = vattrMap[string(key)]
			if !ok {
				return nil, ErrSdUnknownVattrMacro
			}
		}
	} else {
		return nil, ErrUnknownXattrMacro
	}

	return val, nil
}

func (e *Engine) createVbucketDoc() *mockdb.Document {
	v := []byte(fmt.Sprintf(`{"$vbucket":{"HLC":{"mode":"real","now":"%d"}}}`, e.HLC().Unix()))

	return &mockdb.Document{
		Value: v,
	}
}

func (e *Engine) createVattrDoc(doc *mockdb.Document) *mockdb.Document {
	// TODO: revid
	table := crc32.MakeTable(crc32.Castagnoli)

	expiry := int64(0)
	if !doc.Expiry.IsZero() {
		expiry = doc.Expiry.Unix()
	}

	v := []byte{'{'}
	v = append(v, "\"$document\":"...)
	v = append(v, '{')
	v = append(v, fmt.Sprintf("\"exptime\":%d,", expiry)...)
	v = append(v, fmt.Sprintf("\"CAS\":\"0x%016x\",", doc.Cas)...)
	v = append(v, fmt.Sprintf("\"datatype\":%s,", datatypeToString(doc.Datatype))...)
	v = append(v, fmt.Sprintf("\"deleted\":%t,", doc.IsDeleted)...)
	v = append(v, fmt.Sprintf("\"flags\":%d,", doc.Flags)...)
	v = append(v, fmt.Sprintf("\"last_modified\":\"%d\",", doc.ModifiedTime.Unix())...)
	v = append(v, fmt.Sprintf("\"seqno\":\"0x%016x\",", doc.SeqNo)...)
	v = append(v, fmt.Sprintf("\"value_bytes\":%d,", len(doc.Value))...)
	v = append(v, fmt.Sprintf("\"vbucket_uuid\":\"0x%016x\",", doc.VbUUID)...)
	v = append(v, fmt.Sprintf("\"value_crc32c\":\"0x%x\",", crc32.Checksum(doc.Value, table))...)
	v = append(v, fmt.Sprintf("\"revid\":\"%d\"", doc.RevID)...)
	v = append(v, '}')
	v = append(v, '}')

	return &mockdb.Document{
		Value: v,
	}
}

func datatypeToString(datatype uint8) []string {
	if datatype == 0x00 {
		return []string{"\"raw\""}
	}

	var typ []string
	if datatype&0x01 == 1 {
		typ = append(typ, "\"json\"")
	}
	if datatype&0x02 == 1 {
		typ = append(typ, "\"snappy\"")
	}
	if datatype&0x04 == 1 {
		typ = append(typ, "\"xattr\"")
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
