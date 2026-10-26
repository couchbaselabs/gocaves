package mock

// ClusterResources reports which buckets, scopes and collections exist in a
// cluster, for checking the parameters of roles. A memcached bucket has no
// scopes, as ns_server keeps no collections manifest for it.
type ClusterResources struct {
	Cluster Cluster
}

// HasBucket reports whether the bucket exists.
func (r ClusterResources) HasBucket(bucket string) bool {
	return r.Cluster.GetBucket(bucket) != nil
}

// HasScope reports whether the scope exists in the bucket.
func (r ClusterResources) HasScope(bucket, scope string) bool {
	b := r.Cluster.GetBucket(bucket)
	if b == nil || b.BucketType() == BucketTypeMemcached {
		return false
	}
	_, _, err := b.CollectionManifest().GetByName(scope, "")
	return err != ErrScopeNotFound
}

// HasCollection reports whether the collection exists in the scope.
func (r ClusterResources) HasCollection(bucket, scope, collection string) bool {
	b := r.Cluster.GetBucket(bucket)
	if b == nil || b.BucketType() == BucketTypeMemcached {
		return false
	}
	_, _, err := b.CollectionManifest().GetByName(scope, collection)
	return err == nil
}
