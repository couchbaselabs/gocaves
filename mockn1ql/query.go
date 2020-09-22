package mockn1ql

import "github.com/couchbaselabs/gocaves/mockdb"

type Index struct {
	BucketName string
	Fields     []string
}

type Engine struct {
	Indexes []*Index
}

type ExecuteOptions struct {
	Data map[string]*mockdb.Bucket
}

type ExecuteResults struct {
	Rows []interface{}
}

func (e *Engine) Execute(opts ExecuteOptions) (*ExecuteResults, error) {
	return nil, nil
}
