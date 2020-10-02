package mockn1ql

import "github.com/couchbaselabs/gocaves/mock/mockdb"

// Index represents a single view index.
type Index struct {
	BucketName string
	Fields     []string
}

// Engine represents the mock query engine.
type Engine struct {
	Indexes []*Index
}

// ExecuteOptions provides options when executing an query.
type ExecuteOptions struct {
	Data map[string]*mockdb.Bucket
}

// ExecuteResults provides the results from an executed query.
type ExecuteResults struct {
	Rows []interface{}
}

// Execute executes a query.
func (e *Engine) Execute(opts ExecuteOptions) (*ExecuteResults, error) {
	return nil, nil
}
