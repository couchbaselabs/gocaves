package checks

// BucketRef represents a bucket within the checks system.
type BucketRef struct {
	Cluster    ClusterRef
	BucketName string
}

// KvExpectReq returns a new expectation of a kv request.
func (c BucketRef) KvExpectReq() *KvExpect {
	return c.Cluster.KvExpectReq().BucketName(c.BucketName)
}
