package mockmr

import (
	"encoding/json"
	"errors"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/dop251/goja"
	"sort"
	"sync"
)

// Index represents a single map reduce query.
type Index struct {
	Name       string
	MapFunc    string
	ReduceFunc string
}

type DesignDocument struct {
	Name    string
	Indexes []*Index
}

// Engine represents the mock map reduce engine.
type Engine struct {
	designDocuments map[string]*DesignDocument
	lock            sync.Mutex
}

// ExecuteOptions provides options when executing an query.
type ExecuteOptions struct {
	Data      []*mockdb.Document
	DesignDoc string
	View      string

	Skip          int
	StartKey      string
	StartKeyDocID string
	EndKey        string
	EndKeyDocID   string
	InclusiveEnd  bool
	Key           string
	Keys          []string
	Descending    bool
	Reduce        bool
	Group         bool
	GroupLevel    int
	Limit         int
}

// ExecuteResults provides the results from an executed query.
type ExecuteResults struct {
	Rows []outputItem
}

type indexInputMeta struct {
	ID         string `json:"id"`
	Rev        uint64 `json:"rev"`
	Type       uint8  `json:"type"`
	Flags      uint32 `json:"flags"`
	Expiration int    `json:"expiration"`
}

type outputItem struct {
	ID    string
	Key   interface{}
	Value interface{}
}

type indexedItem struct {
	ID    string
	Key   string
	Value interface{}
}

type resultContainer struct {
	results []indexedItem
}

func (rc resultContainer) Len() int {
	return len(rc.results)
}

func (rc resultContainer) Less(i, j int) bool {
	return rc.results[i].Key < rc.results[j].Key
}

func (rc resultContainer) Swap(i, j int) {
	item := rc.results[j]
	rc.results[j] = rc.results[i]
	rc.results[i] = item
}

// NewEngine creates a new Engine
func NewEngine() *Engine {
	return &Engine{
		designDocuments: make(map[string]*DesignDocument),
	}
}

// Execute executes a query.
func (e *Engine) Execute(opts ExecuteOptions) (int, *ExecuteResults, error) {
	ddoc, err := e.GetDesignDocument(opts.DesignDoc)
	if err != nil {
		return 0, nil, err
	}

	var view *Index
	for _, v := range ddoc.Indexes {
		if v.Name == opts.View {
			view = v
			break
		}
	}
	if view == nil {
		return 0, nil, ErrNotFound
	}

	if opts.Reduce && view.ReduceFunc == "" {
		return 0, nil, &InvalidParametersError{
			Message: "{\"error\":\"query_parse_error\",\"reason\":\"Invalid URL parameter `reduce` for map view.\"}",
		}
	}

	indexed, err := e.index(view, opts.Data)
	if err != nil {
		return 0, nil, err
	}

	inclusiveStart := true
	indexSize := len(indexed)
	var results resultContainer

	if opts.Descending {
		startKey := opts.StartKey
		opts.StartKey = opts.EndKey
		opts.EndKey = startKey
		startKeyDocID := opts.StartKeyDocID
		opts.StartKeyDocID = opts.EndKeyDocID
		opts.EndKeyDocID = startKeyDocID
		inclusiveStart = opts.InclusiveEnd
		opts.InclusiveEnd = true
	}

	for _, doc := range indexed {
		if opts.Key != "" {
			if doc.Key != opts.Key {
				continue
			}
		}

		if len(opts.Keys) > 0 {
			var found bool
			for _, k := range opts.Keys {
				if k == doc.Key {
					found = true
					break
				}
			}

			if !found {
				continue
			}
		}

		if inclusiveStart {
			if opts.StartKey != "" && doc.Key < opts.StartKey {
				continue
			}
			if opts.StartKeyDocID != "" && doc.ID < opts.StartKeyDocID {
				continue
			}
		} else {
			if opts.StartKey != "" && doc.Key >= opts.StartKey {
				continue
			}
			if opts.StartKeyDocID != "" && doc.ID >= opts.StartKeyDocID {
				continue
			}
		}

		if opts.InclusiveEnd {
			if opts.EndKey != "" && doc.Key < opts.EndKey {
				continue
			}
			if opts.EndKeyDocID != "" && doc.ID < opts.EndKeyDocID {
				continue
			}
		} else {
			if opts.EndKey != "" && doc.Key >= opts.EndKey {
				continue
			}
			if opts.EndKeyDocID != "" && doc.ID >= opts.EndKeyDocID {
				continue
			}
		}

		result := indexedItem{
			ID:    doc.ID,
			Key:   doc.Key,
			Value: doc.Value,
		}

		results.results = append(results.results, result)
	}

	if opts.Descending {
		sort.Sort(sort.Reverse(results))
	} else {
		sort.Sort(results)
	}

	if opts.Group {
		opts.GroupLevel = -1
	}

	var output []outputItem
	if opts.Reduce {
		output, err = e.reduce(view, opts.GroupLevel, results.results)
		if err != nil {
			return 0, nil, err
		}
	} else {
		for _, item := range results.results {
			var key interface{}
			err := json.Unmarshal([]byte(item.Key), &key)
			if err != nil {
				return 0, nil, err
			}
			output = append(output, outputItem{
				ID:    item.ID,
				Value: item.Value,
				Key:   key,
			})
		}
	}

	output = output[opts.Skip:]
	if opts.Limit > 0 {
		output = output[:opts.Limit]
	}

	return indexSize, &ExecuteResults{Rows: output}, nil
}

func (e *Engine) normalizeKey(key interface{}, groupLevel int) interface{} {
	switch k := key.(type) {
	case []interface{}:
		if groupLevel == -1 {
			return key
		} else if groupLevel == 0 {
			return nil
		}
		return k[:groupLevel]
	}

	return key
}

func (e *Engine) reduce(index *Index, groupLevel int, docs []indexedItem) ([]outputItem, error) {
	vm := goja.New()

	var reduceFn string
	if reducer, ok := builtinViewReducers[index.ReduceFunc]; ok {
		reduceFn = reducer
	} else {
		reduceFn = index.ReduceFunc
	}

	reduceFn = "reduce = " + reduceFn

	_, err := vm.RunString(reduceFn)
	if err != nil {
		return nil, err
	}

	fn, ok := goja.AssertFunction(vm.Get("reduce"))
	if !ok {
		return nil, errors.New("cannot parse function")
	}

	var results []outputItem
	keyMap := make(map[interface{}][]interface{})
	for _, doc := range docs {
		var k interface{}
		err := json.Unmarshal([]byte(doc.Key), &k)
		if err != nil {
			return nil, err
		}
		key := e.normalizeKey(k, groupLevel)
		if _, ok := keyMap[key]; !ok {
			keyMap[key] = []interface{}{}
		}
		keyMap[key] = append(keyMap[key], doc.Value)
	}

	for key, vals := range keyMap {
		result, err := fn(goja.Undefined(), vm.ToValue(key), vm.ToValue(vals), vm.ToValue(false))
		if err != nil {
			return nil, err
		}

		results = append(results, outputItem{
			Key:   key,
			Value: result,
		})
	}

	return results, nil
}

func (e *Engine) index(index *Index, docs []*mockdb.Document) ([]indexedItem, error) {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	var data []indexedItem

	var currentDoc *mockdb.Document
	emit := func(key interface{}, value interface{}) {
		k, err := json.Marshal(key)
		if err != nil {
			panic(err)
		}
		data = append(data, indexedItem{
			Key:   string(k),
			ID:    string(currentDoc.Key),
			Value: value,
		})
	}

	err := vm.Set("emit", emit)
	if err != nil {
		return nil, err
	}

	fnStr := "callme = " + index.MapFunc
	_, err = vm.RunString(fnStr)
	if err != nil {
		return nil, err
	}

	fn, ok := goja.AssertFunction(vm.Get("callme"))
	if !ok {
		return nil, errors.New("cannot parse function")
	}

	for _, doc := range docs {
		currentDoc = doc

		var docValue map[string]interface{}
		err := json.Unmarshal(doc.Value, &docValue)
		if err != nil || docValue == nil {
			// TODO: this should probably do something else, non json docs are supported by views.
			continue
		}

		meta := indexInputMeta{
			ID:         string(doc.Key),
			Rev:        0,
			Type:       doc.Datatype,
			Flags:      doc.Flags,
			Expiration: doc.Expiry.Second(),
		}

		_, err = fn(goja.Undefined(), vm.ToValue(docValue), vm.ToValue(meta))
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

type UpsertDesignDocumentOptions struct {
	Indexes []*Index
}

// UpsertDesignDocument creates or updates a design document.
func (e *Engine) UpsertDesignDocument(name string, opts UpsertDesignDocumentOptions) error {
	ddoc := &DesignDocument{
		Name:    name,
		Indexes: opts.Indexes,
	}
	e.lock.Lock()
	e.designDocuments[ddoc.Name] = ddoc
	e.lock.Unlock()

	return nil
}

// DropDesignDocument removes a design document.
func (e *Engine) DropDesignDocument(name string) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	if _, ok := e.designDocuments[name]; !ok {
		return ErrNotFound
	}
	delete(e.designDocuments, name)
	return nil
}

// GetDesignDocument retrieves a single design document.
func (e *Engine) GetDesignDocument(name string) (*DesignDocument, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if ddoc, ok := e.designDocuments[name]; ok {
		return ddoc, nil
	}

	return nil, ErrNotFound
}

// GetAllDesignDocuments retrieves all design documents.
func (e *Engine) GetAllDesignDocuments() []*DesignDocument {
	e.lock.Lock()
	ddocMap := e.designDocuments
	e.lock.Unlock()
	var ddocs []*DesignDocument
	for _, ddoc := range ddocMap {
		ddocs = append(ddocs, ddoc)
	}

	return ddocs
}
