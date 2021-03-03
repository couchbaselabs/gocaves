package mockmr

import (
	"github.com/couchbaselabs/gocaves/mock/mockdb"
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
	Data *mockdb.Bucket
}

// ExecuteResults provides the results from an executed query.
type ExecuteResults struct {
	Rows []interface{}
}

// NewEngine creates a new Engine
func NewEngine() *Engine {
	return &Engine{
		designDocuments: make(map[string]*DesignDocument),
	}
}

// Execute executes a query.
func (e *Engine) Execute(opts ExecuteOptions) (*ExecuteResults, error) {
	return nil, nil
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
