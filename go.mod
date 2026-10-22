module github.com/couchbaselabs/gocaves

go 1.26.0

require (
	github.com/couchbase/gocb/v2 v2.1.6
	github.com/couchbase/gocbcore/v9 v9.0.6
	github.com/couchbaselabs/gocaves/client v0.0.0-20211201195517-a39ea9ff2037
	github.com/dop251/goja v0.0.0-20210427212725-462d53687b0d
	github.com/go-bindata/go-bindata v3.1.2+incompatible
	github.com/google/uuid v1.1.1
	github.com/stretchr/testify v1.5.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dlclark/regexp2 v1.4.1-0.20201116162257-a2a8dda75c91 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.1.1 // indirect
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/couchbaselabs/gocaves/client => ./client
