package mock

import "github.com/couchbaselabs/gocaves/mock/mocktime"

type BucketStore interface {
	Chrono() *mocktime.Chrono
}
