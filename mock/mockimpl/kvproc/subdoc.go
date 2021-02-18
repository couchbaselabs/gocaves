package kvproc

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"hash/crc32"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

const subdocMultiMaxPaths = 16

func (e *Engine) executeSdOps(doc, newMeta *mockdb.Document, ops []*SubDocOp) ([]*SubDocResult, error) {
	if len(ops) > subdocMultiMaxPaths {
		return nil, ErrSdBadCombo
	}

	opReses := make([]*SubDocResult, len(ops))

	for opIdx, op := range ops {
		var opDoc *mockdb.Document
		if op.IsXattrPath {
			// TODO: This should maybe move up to the operations level at some point?
			var err error
			opDoc, err = e.createXattrDoc(doc, op)
			if err != nil {
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
			newMeta: newMeta,
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
		case memd.SubDocOpAddDoc:
		}

		if executor == nil {
			return nil, ErrInternal
		}

		opRes, err := executor.Execute(op)

		if err != nil {
			return nil, err
		}
		if opRes == nil {
			return nil, ErrInternal
		}

		if op.IsXattrPath && opRes.Err == nil && subdocOpIsMutation(op) {
			pathComps, err := ParseSubDocPath(op.Path)
			if err != nil {
				// It'd be very strange to actually get here.
				opReses[opIdx] = &SubDocResult{
					Value: nil,
					Err:   err,
				}
				continue
			}

			key := pathComps[0].Path

			// We created a fake document for the xattr so we need to strip it down to only the value.
			v := opDoc.Value[len(fmt.Sprintf("{\"%s\":", key)):]
			v = v[:len(v)-1] // }
			doc.Xattrs[key] = v
		}

		opReses[opIdx] = opRes
	}

	return opReses, nil
}

func (e *Engine) createXattrDoc(doc *mockdb.Document, op *SubDocOp) (*mockdb.Document, error) {
	if err := validateXattrPath(op); err != nil {
		return nil, err
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return nil, err
	}

	key := pathComps[0].Path

	if key == "$document" {
		if subdocOpIsMutation(op) {
			return nil, ErrSdCannotModifyVattr
		}

		return e.createVattrDoc(doc), nil
	} else if key == "$vbucket" {
		if subdocOpIsMutation(op) {
			return nil, ErrSdCannotModifyVattr
		}

		return e.createVbucketDoc(), nil
	}

	xattr, ok := doc.Xattrs[key]
	if !ok {
		return &mockdb.Document{
			Value: []byte("{}"),
		}, nil
	}

	v := []byte(fmt.Sprintf("{\"%s\":", key))
	v = append(v, xattr...)
	v = append(v, '}')

	return &mockdb.Document{
		Value: v,
	}, nil
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
	v = append(v, fmt.Sprintf("\"value_crc32c\":\"0x%x\"", crc32.Checksum(doc.Value, table))...)
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

func validateXattrPath(op *SubDocOp) error {
	trimmedPath := strings.TrimSpace(op.Path)
	if trimmedPath == "" {
		return ErrSdInvalidXattr
	}
	if trimmedPath == "[" {
		return ErrSdInvalidXattr
	}

	return nil
}

// SubDocExecutor is an executor for subdocument operations.
type SubDocExecutor interface {
	Execute(op *SubDocOp) (*SubDocResult, error)
}

type baseSubDocExecutor struct {
	doc     *mockdb.Document
	newMeta *mockdb.Document
}

func (e baseSubDocExecutor) itemErrorResult(err error) (*SubDocResult, error) {
	return &SubDocResult{
		Value: nil,
		Err:   err,
	}, nil
}

// SubDocGetExecutor is an executor for subdocument get operations.
type SubDocGetExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathBytes, err := pathVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: pathBytes,
		Err:   nil,
	}, nil
}

// SubDocExistsExecutor is an executor for subdocument operations.
type SubDocExistsExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocExistsExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	_, err = pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocGetCountExecutor is an executor for subdocument operations.
type SubDocGetCountExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetCountExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathData, err := pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	elemCount := 0
	switch typedPathData := pathData.(type) {
	case []interface{}:
		elemCount = len(typedPathData)
	case map[string]interface{}:
		elemCount = len(typedPathData)
	default:
		return e.itemErrorResult(ErrSdPathMismatch)
	}

	countBytes := []byte(fmt.Sprintf("%d", elemCount))

	return &SubDocResult{
		Value: countBytes,
		Err:   nil,
	}, nil
}

// SubDocGetDocExecutor is an executor for subdocument operations.
type SubDocGetDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetDocExecutor) Execute(_ *SubDocOp) (*SubDocResult, error) {
	// TODO: check what happens with a full doc against an xattr
	return &SubDocResult{
		Value: e.doc.Value,
		Err:   nil,
	}, nil
}

func doBasicMutation(doc []byte, op *SubDocOp, mutationFn func(pathVal *subDocManip, valueObj interface{}) error) ([]byte, error) {
	docVal, err := newSubDocManip(doc)
	if err != nil {
		return nil, err
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return nil, err
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return nil, err
	}

	err = mutationFn(pathVal, valueObj)
	if err != nil {
		return nil, err
	}

	return docVal.GetJSON()
}

// SubDocDictSetExecutor is an executor for subdocument operations.
type SubDocDictSetExecutor struct {
	baseSubDocExecutor
}

// Execute runs this SubDocDictSetExecutor
func (e SubDocDictSetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	var err error
	e.doc.Value, err = doBasicMutation(e.doc.Value, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Set(valueObj)
	})
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDictAddExecutor is an executor for subdocument operations.
type SubDocDictAddExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDictAddExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	var err error
	e.doc.Value, err = doBasicMutation(e.doc.Value, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Insert(valueObj)
	})
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocReplaceExecutor is an executor for subdocument operations.
type SubDocReplaceExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocReplaceExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	var err error
	e.doc.Value, err = doBasicMutation(e.doc.Value, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Replace(valueObj)
	})
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDeleteExecutor is an executor for subdocument operations.
type SubDocDeleteExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return e.itemErrorResult(err)
	}
	if len(pathComps) <= 0 {
		// Need to at least specify the index to insert at.
		return e.itemErrorResult(ErrSdPathInvalid)
	}

	lastPathComp := pathComps[len(pathComps)-1]

	// Calculate the path without the array index
	deOpPath := StringifySubDocPath(pathComps[:len(pathComps)-1])

	pathVal, err := docVal.GetByPath(deOpPath, op.CreatePath, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	val, err := pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	if lastPathComp.Path != "" {
		// Map
		mapVal, isMap := val.(map[string]interface{})
		if !isMap {
			return e.itemErrorResult(ErrSdPathMismatch)
		}

		_, hasElem := mapVal[lastPathComp.Path]
		if !hasElem {
			return e.itemErrorResult(ErrSdPathNotFound)
		}

		delete(mapVal, lastPathComp.Path)

		err = pathVal.Replace(mapVal)
		if err != nil {
			return e.itemErrorResult(err)
		}
	} else {
		// Array
		arrVal, isArr := val.([]interface{})
		if !isArr {
			return e.itemErrorResult(ErrSdPathMismatch)
		}

		if lastPathComp.ArrayIndex < 0 {
			lastPathComp.ArrayIndex = len(arrVal) + lastPathComp.ArrayIndex
		}
		if lastPathComp.ArrayIndex >= len(arrVal) {
			return e.itemErrorResult(ErrSdPathNotFound)
		}

		newArrVal := make([]interface{}, 0)
		newArrVal = append(newArrVal, arrVal[:lastPathComp.ArrayIndex]...)
		newArrVal = append(newArrVal, arrVal[lastPathComp.ArrayIndex+1:]...)

		err = pathVal.Replace(newArrVal)
		if err != nil {
			return e.itemErrorResult(err)
		}
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocCounterExecutor is an executor for subdocument operations.
type SubDocCounterExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocCounterExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	var delta float64
	err = json.Unmarshal(op.Value, &delta)
	if err != nil {
		return e.itemErrorResult(err)
	}

	// TODO(brett19): Check the behaviour when the path doesn't exist.

	val, err := pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	floatVal, isFloat := val.(float64)
	if !isFloat {
		return e.itemErrorResult(ErrSdPathMismatch)
	}

	newVal := int64(floatVal + delta)

	err = pathVal.Set(newVal)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: []byte(strconv.FormatInt(newVal, 10)),
		Err:   nil,
	}, nil
}

func subDocArrayPush(docVal *subDocManip, op *SubDocOp, pushFront bool) error {
	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
	if err != nil {
		return err
	}

	var fullValue []interface{}
	fullValueBytes := append([]byte("["), append(append([]byte{}, op.Value...), []byte("]")...)...)
	err = json.Unmarshal(fullValueBytes, &fullValue)
	if err != nil {
		return err
	}

	val, err := pathVal.Get()
	if err != nil {
		if errors.Is(err, ErrSdPathNotFound) {
			val = []interface{}{}
			err = pathVal.Set(val)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return ErrSdPathMismatch
	}

	if pushFront {
		arrVal = append(fullValue, arrVal...)
	} else {
		arrVal = append(arrVal, fullValue...)
	}

	return pathVal.Replace(arrVal)
}

// SubDocArrayPushFirstExecutor is an executor for subdocument operations.
type SubDocArrayPushFirstExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushFirstExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, true)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocArrayPushLastExecutor is an executor for subdocument operations.
type SubDocArrayPushLastExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushLastExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocArrayInsertExecutor is an executor for subdocument operations.
type SubDocArrayInsertExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayInsertExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return e.itemErrorResult(err)
	}
	if len(pathComps) <= 0 {
		// Need to at least specify the index to insert at.
		return e.itemErrorResult(ErrSdPathInvalid)
	}

	lastPathComp := pathComps[len(pathComps)-1]
	if lastPathComp.Path != "" {
		// Last index should be an array index.
		return e.itemErrorResult(ErrSdPathInvalid)
	}

	// Calculate the path without the array index
	deOpPath := StringifySubDocPath(pathComps[:len(pathComps)-1])

	pathVal, err := docVal.GetByPath(deOpPath, op.CreatePath, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	var fullValue []interface{}
	fullValueBytes := append([]byte("["), append(append([]byte{}, op.Value...), []byte("]")...)...)
	err = json.Unmarshal(fullValueBytes, &fullValue)
	if err != nil {
		return e.itemErrorResult(err)
	}

	val, err := pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return e.itemErrorResult(ErrSdPathMismatch)
	}

	if lastPathComp.ArrayIndex < 0 {
		lastPathComp.ArrayIndex = len(arrVal) + lastPathComp.ArrayIndex
	}
	if lastPathComp.ArrayIndex >= len(arrVal) {
		// Cant insert past the end of the array
		return e.itemErrorResult(ErrSdPathMismatch)
	}

	var newArrVal []interface{}
	newArrVal = append(newArrVal, arrVal[:lastPathComp.ArrayIndex]...)
	newArrVal = append(newArrVal, fullValue...)
	newArrVal = append(newArrVal, arrVal[lastPathComp.ArrayIndex:]...)

	err = pathVal.Replace(newArrVal)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocArrayAddUniqueExecutor is an executor for subdocument operations.
type SubDocArrayAddUniqueExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayAddUniqueExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
	if err != nil {
		return e.itemErrorResult(err)
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return e.itemErrorResult(err)
	}

	val, err := pathVal.Get()
	if err != nil {
		return e.itemErrorResult(err)
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return e.itemErrorResult(ErrSdPathMismatch)
	}

	foundExisting := false
	for _, arrElem := range arrVal {
		if reflect.DeepEqual(arrElem, valueObj) {
			foundExisting = true
			break
		}
	}

	if foundExisting {
		return e.itemErrorResult(ErrSdPathExists)
	}

	arrVal = append(arrVal, valueObj)

	err = pathVal.Replace(arrVal)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDeleteFullDocExecutor is an executor for subdocument operations.
type SubDocDeleteFullDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteFullDocExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.Path != "" {
		return e.itemErrorResult(ErrInvalidArgument)
	}
	if len(op.Value) != 0 {
		return e.itemErrorResult(ErrInvalidArgument)
	}

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
	if op.Path != "" {
		return e.itemErrorResult(ErrInvalidArgument)
	}

	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return e.itemErrorResult(err)
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return e.itemErrorResult(err)
	}

	err = pathVal.Set(valueObj)
	if err != nil {
		return e.itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return e.itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}
