package mock

// BucketType specifies the type of bucket
type BucketType uint

// The following lists the possible bucket types
const (
	BucketTypeMemcached = BucketType(1)
	BucketTypeCouchbase = BucketType(2)
)

// NewBucketOptions allows you to specify initial options for a new bucket
type NewBucketOptions struct {
	Name        string
	Type        BucketType
	NumReplicas uint
	NumVbuckets uint
}

// Bucket represents an instance of a Bucket
type Bucket struct {
	cluster     *Cluster
	name        string
	bucketType  BucketType
	numReplicas uint
	numVbuckets uint
}

func newBucket(parent *Cluster, opts NewBucketOptions) (*Bucket, error) {
	bucket := &Bucket{
		cluster:     parent,
		name:        opts.Name,
		bucketType:  opts.Type,
		numReplicas: opts.NumReplicas,
	}

	return bucket, nil
}

// Name returns the name of this bucket
func (b Bucket) Name() string {
	return b.name
}

// BucketType returns the type of bucket this is.
func (b Bucket) BucketType() BucketType {
	return b.bucketType
}

// NumReplicas returns the number of configured replicas for this bucket
func (b Bucket) NumReplicas() uint {
	return b.numReplicas
}
