package checks

// ScopeRef represents a scope within the checks system.
type ScopeRef struct {
	Bucket    BucketRef
	ScopeName string
}

// KvExpectReq returns a new expectation of a kv request.
func (c ScopeRef) KvExpectReq() *KvExpect {
	return c.Bucket.KvExpectReq().ScopeName(c.ScopeName)
}
