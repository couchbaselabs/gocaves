module github.com/couchbaselabs/gocaves

go 1.26.0

require (
	github.com/couchbase/gocb/v2 v2.13.0
	github.com/couchbase/gocbcore/v10 v10.10.0
	github.com/couchbaselabs/gocaves/client v0.0.0-20250107114554-f96479220ae8
	github.com/dop251/goja v0.0.0-20210427212725-462d53687b0d
	github.com/go-bindata/go-bindata v3.1.2+incompatible
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/couchbase/gocbcoreps v0.1.5-0.20261005161736-7de374b343fa // indirect
	github.com/couchbase/goprotostellar v1.0.6-0.20261005130525-13bdd85cd6ce // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0 // indirect
	github.com/dlclark/regexp2 v1.4.1-0.20201116162257-a2a8dda75c91 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.71.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/couchbaselabs/gocaves/client => ./client
