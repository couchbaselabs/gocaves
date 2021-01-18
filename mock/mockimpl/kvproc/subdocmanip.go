package kvproc

import (
	"encoding/json"
	"errors"
)

type subDocManip struct {
	root interface{}
	path interface{}
}

func newSubDocManip(data []byte) (*subDocManip, error) {
	if len(data) == 0 {
		return &subDocManip{
			root: nil,
			path: nil,
		}, nil
	}

	var val interface{}
	err := json.Unmarshal(data, &val)
	if err != nil {
		return nil, err
	}

	return &subDocManip{
		root: val,
		path: nil,
	}, nil
}

func (m *subDocManip) getByPathComp(comp *SubDocPathComponent, createPath bool) (*subDocManip, error) {
	newRoot, err := m.Get()
	if err == ErrSdPathNotFound && createPath {
		newRoot = make(map[string]interface{})
		err = m.Set(newRoot)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	var newPath interface{}
	if comp.Path != "" {
		_, isMap := newRoot.(map[string]interface{})
		if !isMap {
			return nil, ErrSdPathMismatch
		}

		newPath = comp.Path
	} else {
		_, isArr := newRoot.([]interface{})
		if !isArr {
			return nil, ErrSdPathMismatch
		}

		newPath = comp.ArrayIndex
	}

	return &subDocManip{
		root: newRoot,
		path: newPath,
	}, nil
}

func (m *subDocManip) GetByPath(path string, createPath, createLast bool) (*subDocManip, error) {
	if path == "" {
		return m, nil
	}

	pathComps, err := ParseSubDocPath(path)
	if err != nil {
		return nil, ErrSdPathInvalid
	}

	manipIter := m
	for compIdx, comp := range pathComps {
		createComp := createPath
		if createLast && compIdx == len(pathComps)-1 {
			// This is the last component, and createLast is enabled
			createComp = true
		}

		manipIter, err = manipIter.getByPathComp(&comp, createComp)
		if err != nil {
			return nil, err
		}
	}

	return manipIter, nil
}

func (m *subDocManip) Get() (interface{}, error) {
	switch typedPath := m.path.(type) {
	case string:
		mapType := m.root.(map[string]interface{})
		pathVal, hasElem := mapType[typedPath]
		if !hasElem {
			return nil, ErrSdPathNotFound
		}

		return pathVal, nil
	case int:
		arrType := m.root.([]interface{})
		if typedPath < 0 {
			if len(arrType)+typedPath < 0 {
				return nil, ErrSdPathNotFound
			}
			return arrType[len(arrType)+typedPath], nil
		}

		if typedPath >= len(arrType) {
			return nil, ErrSdPathNotFound
		}
		return arrType[typedPath], nil
	case nil:
		return m.root, nil
	default:
		return nil, errors.New("unexpected path type")
	}
}

func (m *subDocManip) Set(val interface{}) error {
	switch typedPath := m.path.(type) {
	case string:
		typedRoot := m.root.(map[string]interface{})
		typedRoot[typedPath] = val
		return nil
	case int:
		typedRoot := m.root.([]interface{})
		if typedPath < 0 {
			typedRoot[len(typedRoot)+typedPath] = val
			return nil
		}
		if typedPath >= len(typedRoot) {
			return ErrSdPathNotFound
		}
		typedRoot[typedPath] = val
		return nil
	case nil:
		m.root = val
		return nil
	default:
		return errors.New("unexpected path type")
	}
}

func (m *subDocManip) Replace(val interface{}) error {
	switch typedPath := m.path.(type) {
	case string:
		typedRoot := m.root.(map[string]interface{})
		if _, hasElem := typedRoot[typedPath]; !hasElem {
			return ErrSdPathNotFound
		}

		typedRoot[typedPath] = val
		return nil
	case int:
		typedRoot := m.root.([]interface{})
		if typedPath < 0 {
			typedRoot[len(typedRoot)+typedPath] = val
			return nil
		}
		if typedPath >= len(typedRoot) {
			return ErrSdPathNotFound
		}
		typedRoot[typedPath] = val
		return nil
	case nil:
		m.root = val
		return nil
	default:
		return errors.New("unexpected path type")
	}
}

func (m *subDocManip) Insert(val interface{}) error {
	switch typedPath := m.path.(type) {
	case string:
		typedRoot := m.root.(map[string]interface{})
		if _, hasElem := typedRoot[typedPath]; hasElem {
			return ErrSdPathExists
		}

		typedRoot[typedPath] = val
		return nil
	case int:
		return ErrSdPathMismatch
	case nil:
		return ErrSdPathMismatch
	default:
		return errors.New("unexpected path type")
	}
}

func (m *subDocManip) GetJSON() ([]byte, error) {
	val, err := m.Get()
	if err != nil {
		return nil, err
	}

	return json.Marshal(val)
}
