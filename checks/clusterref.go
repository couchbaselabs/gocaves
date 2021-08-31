package checks

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// ClusterRef represents a cluster within the checks system.
type ClusterRef struct {
	t *T
}

// KvExpectReq returns a new expectation of a kv request.
func (c ClusterRef) KvExpectReq() *KvExpect {
	return (&KvExpect{
		parent: c.t,
	}).Magic(memd.CmdMagicReq)
}

// KvExpectReq returns a new expectation of a kv response.
func (c ClusterRef) KvExpectRes() *KvExpect {
	return (&KvExpect{
		parent: c.t,
	}).Magic(memd.CmdMagicRes)
}

func (c ClusterRef) HookHelper(handler mock.KvHookFunc) KVHook {
	return NewKvHook(c.KvExpectReq(), handler)
}
