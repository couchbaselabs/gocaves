package mockmr

import "github.com/couchbaselabs/gocaves/mockdb"

// Index represents a single map reduce query.
type Index struct {
	MapFunc    string
	ReduceFunc string
}

// Engine represents the mock map reduce engine.
type Engine struct {
	Indexes []Index
}

// ExecuteOptions provides options when executing an query.
type ExecuteOptions struct {
	Data *mockdb.Bucket
}

// ExecuteResults provides the results from an executed query.
type ExecuteResults struct {
	Rows []interface{}
}

// Execute executes a query.
func (e *Engine) Execute(opts ExecuteOptions) (*ExecuteResults, error) {
	return nil, nil
}
