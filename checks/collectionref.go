package checks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// CollectionRef represents a collection within the checks system.
type CollectionRef struct {
	Scope          ScopeRef
	CollectionName string
}

// KvExpectReq returns a new expectation of a kv request.
func (c CollectionRef) KvExpectReq() *KvExpect {
	return c.Scope.KvExpectReq().CollectionName(c.CollectionName)
}

func (c CollectionRef) HookHelper(handler mock.KvHookFunc) KVHook {
	return NewKvHook(c.KvExpectReq(), handler)
}
