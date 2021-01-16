package kvproc

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

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
		var opRes *SubDocResult
		var err error

		switch op.Op {
		case memd.SubDocOpGet:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGet(op)
		case memd.SubDocOpExists:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpExists(op)
		case memd.SubDocOpGetCount:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGetCount(op)
		case memd.SubDocOpGetDoc:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpGetDoc(op)

		case memd.SubDocOpDictAdd:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDictAdd(op)
		case memd.SubDocOpDictSet:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDictSet(op)
		case memd.SubDocOpDelete:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDelete(op)
		case memd.SubDocOpReplace:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpReplace(op)
		case memd.SubDocOpArrayPushLast:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpArrayPushLast(op)
		case memd.SubDocOpArrayPushFirst:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpArrayPushFirst(op)
		case memd.SubDocOpArrayInsert:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpArrayInsert(op)
		case memd.SubDocOpArrayAddUnique:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpArrayAddUnique(op)
		case memd.SubDocOpCounter:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpCounter(op)
		case memd.SubDocOpSetDoc:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDictSetFullDoc(op)
		case memd.SubDocOpDeleteDoc:
			opRes, err = SubDocExecutor{doc, newMeta}.executeSdOpDeleteFullDoc(op)
		case memd.SubDocOpAddDoc:
		}

		if err != nil {
			return nil, err
		}
		if opRes == nil {
			return nil, ErrInternal
		}

		opReses[opIdx] = opRes
	}

	return opReses, nil
}

// SubDocExecutor is an executor for subdocument operations.
type SubDocExecutor struct {
	doc     *mockdb.Document
	newMeta *mockdb.Document
}

func (e SubDocExecutor) itemErrorResult(err error) (*SubDocResult, error) {
	return &SubDocResult{
		Value: nil,
		Err:   err,
	}, nil
}

func (e SubDocExecutor) executeSdOpGet(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpExists(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpGetCount(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpGetDoc(op *SubDocOp) (*SubDocResult, error) {
	return &SubDocResult{
		Value: e.doc.Value,
		Err:   nil,
	}, nil
}

func (e SubDocExecutor) executeSdOpDictSet(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpDictAdd(op *SubDocOp) (*SubDocResult, error) {
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

	err = pathVal.Insert(valueObj)
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

func (e SubDocExecutor) executeSdOpReplace(op *SubDocOp) (*SubDocResult, error) {
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

	err = pathVal.Replace(valueObj)
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

func (e SubDocExecutor) executeSdOpDelete(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpCounter(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpArrayPushFirst(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
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

	arrVal = append(fullValue, arrVal...)

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

func (e SubDocExecutor) executeSdOpArrayPushLast(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return e.itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
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

	arrVal = append(arrVal, fullValue...)

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

func (e SubDocExecutor) executeSdOpArrayInsert(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpArrayAddUnique(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpDeleteFullDoc(op *SubDocOp) (*SubDocResult, error) {
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

func (e SubDocExecutor) executeSdOpDictSetFullDoc(op *SubDocOp) (*SubDocResult, error) {
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
