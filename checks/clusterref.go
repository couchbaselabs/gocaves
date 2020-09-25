package checks

// ClusterRef represents a cluster within the checks system.
type ClusterRef struct {
	t *T
}

// KvExpectReq returns a new expectation of a kv request.
func (c ClusterRef) KvExpectReq() *KvExpect {
	return &KvExpect{
		parent: c.t,
	}
}
