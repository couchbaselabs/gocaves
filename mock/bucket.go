package mock

import "github.com/couchbaselabs/gocaves/mock/mockdb"

// BucketType specifies the type of bucket
type BucketType uint

// The following lists the possible bucket types
const (
	BucketTypeMemcached       = BucketType(1)
	BucketTypeCouchbase       = BucketType(2)
	BucketTypeEphemeral       = BucketType(3)
	BucketTypeMemcachedString = "memcached"
	BucketTypeCouchbaseString = "membase"
	BucketTypeEphemeralString = "ephemeral"
)

func (b BucketType) Name() string {
	switch b {
	case BucketTypeMemcached:
		return BucketTypeMemcachedString
	case BucketTypeCouchbase:
		return BucketTypeCouchbaseString
	case BucketTypeEphemeral:
		return BucketTypeEphemeralString
	}

	return ""
}

func BucketTypeFromString(bucketTypeString string) BucketType {
	switch bucketTypeString {
	case BucketTypeMemcachedString:
		return BucketTypeMemcached
	case BucketTypeCouchbaseString:
		return BucketTypeCouchbase
	case BucketTypeEphemeralString:
		return BucketTypeEphemeral
	}

	// Default to Couchbase?
	return BucketTypeCouchbase
}

// CompressionMode specifies the kind of compression to use for a bucket.
type CompressionMode string

const (
	// CompressionModeOff specifies to use no compression for a bucket.
	CompressionModeOff CompressionMode = "off"

	// CompressionModePassive specifies to use passive compression for a bucket.
	CompressionModePassive CompressionMode = "passive"

	// CompressionModeActive specifies to use active compression for a bucket.
	CompressionModeActive CompressionMode = "active"
)

// NewBucketOptions allows you to specify initial options for a new bucket
type NewBucketOptions struct {
	Name                string
	Type                BucketType
	NumReplicas         uint
	FlushEnabled        bool
	RamQuota            uint64
	ReplicaIndexEnabled bool
	CompressionMode     CompressionMode
}

// UpdateBucketOptions allows you to specify options for updating a bucket
type UpdateBucketOptions struct {
	NumReplicas         uint
	FlushEnabled        bool
	RamQuota            uint64
	ReplicaIndexEnabled bool
	CompressionMode     CompressionMode
}

// Bucket represents an instance of a bucket.
type Bucket interface {
	// ID returns the uuid of this bucket.
	ID() string

	// Name returns the name of this bucket
	Name() string

	// BucketType returns the type of bucket this is.
	BucketType() BucketType

	// NumReplicas returns the number of configured replicas for this bucket
	NumReplicas() uint

	// ConfigRev returns the current configuration revision for this bucket.
	ConfigRev() uint

	// CollectionManifest returns the collection manifest of this bucket.
	CollectionManifest() *CollectionManifest

	// Store returns the data-store for this bucket.
	Store() *mockdb.Bucket

	// UpdateVbMap will update the vbmap such that all vbuckets are assigned to the
	// specific nodes which are passed in.  Note that this rebalance is guarenteed to
	// be very explicit such that vbNode = (vbId % numNode), and replicas are just ++.
	UpdateVbMap(nodeList []string)

	// GetVbServerInfo returns the vb nodes, then the vb map, then the ordered list of all nodes
	GetVbServerInfo(reqNode ClusterNode) ([]ClusterNode, [][]int, []ClusterNode)

	// VbucketOwnership returns the replica index associated with the provided node.
	// A response of -1 means it does not own any replicas for that vbucket.
	VbucketOwnership(node ClusterNode) []int

	// ViewIndexManager returns the view index manager for this bucket.
	ViewIndexManager() ViewIndexManager

	// Flush will remove all items from containing vbuckets and reset the high seq no.
	// Returning true if the flush was successful.
	Flush()

	// FlushEnabled returns whether or not flush is enabled for this bucket.
	FlushEnabled() bool

	// RamQuota returns the ram quota assigned to this bucket.
	RamQuota() uint64

	// ReplicaIndexEnabled returns whether or not replica index is enabled for this bucket.
	ReplicaIndexEnabled() bool

	// Update updates the settings for this bucket.
	Update(opts UpdateBucketOptions) error

	// CompressionMode returns the compression mode used by this bucket.
	CompressionMode() CompressionMode
}
