package crudproc

import (
	"encoding/json"
	"fmt"

	"github.com/couchbaselabs/gocaves/mockdb"
)

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

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return e.itemErrorResult(err)
	}

	err = pathVal.Remove()
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
