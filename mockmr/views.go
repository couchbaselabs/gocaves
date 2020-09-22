package mockmr

import "github.com/couchbaselabs/gocaves/mockdb"

type Index struct {
	MapFunc    string
	ReduceFunc string
}

type Engine struct {
	Indexes []Index
}

type ExecuteOptions struct {
	Data *mockdb.Bucket
}

type ExecuteResults struct {
	Rows []interface{}
}

func (e *Engine) Execute(opts ExecuteOptions) (*ExecuteResults, error) {
	return nil, nil
}
