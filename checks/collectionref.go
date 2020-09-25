package checks

// CollectionRef represents a collection within the checks system.
type CollectionRef struct {
	Scope          ScopeRef
	CollectionName string
}

// KvExpectReq returns a new expectation of a kv request.
func (c CollectionRef) KvExpectReq() *KvExpect {
	return c.Scope.KvExpectReq().CollectionName(c.CollectionName)
}
