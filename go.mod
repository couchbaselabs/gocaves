module github.com/couchbaselabs/gocaves

go 1.13

replace github.com/couchbase/gocbcore/v9 => /Users/brettlawson/couchsdk/gocbcore

require (
	github.com/couchbase/gocb/v2 v2.1.6
	github.com/couchbase/gocbcore/v9 v9.0.6
	github.com/couchbaselabs/gocb v1.6.7 // indirect
	github.com/google/uuid v1.1.1
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/stretchr/testify v1.5.1
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.17 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.0 // indirect
)
