package checks

import "github.com/couchbaselabs/gocaves/mock"

// BucketRef represents a bucket within the checks system.
type BucketRef struct {
	Cluster    ClusterRef
	BucketName string
}

// KvExpectReq returns a new expectation of a kv request.
func (c BucketRef) KvExpectReq() *KvExpect {
	return c.Cluster.KvExpectReq().BucketName(c.BucketName)
}

func (c BucketRef) HookHelper(handler mock.KvHookFunc) KVHook {
	return NewKvHook(c.KvExpectReq(), handler)
}
