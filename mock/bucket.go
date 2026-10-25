package mock

import (
	"encoding/json"
	"errors"

	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

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
	// MaxTTL is the bucket's maxTTL in seconds; 0 sets none.
	MaxTTL uint32
	// Settings holds the settings the mock keeps and reports only. Zero
	// fields take ns_server's defaults for the bucket type.
	Settings BucketSettings
}

// UpdateBucketOptions allows you to specify options for updating a bucket.
// A nil field leaves the bucket's setting as it is; Settings replaces all
// the settings it holds.
type UpdateBucketOptions struct {
	NumReplicas         *uint
	FlushEnabled        *bool
	RamQuota            *uint64
	ReplicaIndexEnabled *bool
	CompressionMode     *CompressionMode
	MaxTTL              *uint32
	Settings            *BucketSettings
}

// BucketSettings holds the bucket settings ns_server keeps and reports in
// the bucket config, which the mock does not act on.
type BucketSettings struct {
	EvictionPolicy     string
	StorageBackend     string
	DurabilityMinLevel string
	// ConflictResolutionType is seqno or lww; a lww bucket also has the
	// drift thresholds, in milliseconds.
	ConflictResolutionType string
	DriftAheadThresholdMs  json.Number
	DriftBehindThresholdMs json.Number
	NumThreads             json.Number
	// PurgeInterval is the metadata purge interval in days, empty if the
	// bucket has none of its own.
	PurgeInterval  json.Number
	AutoCompaction *AutoCompactionSettings
}

// AutoCompactionSettings are the auto-compaction settings of a bucket. A
// nil threshold is undefined.
type AutoCompactionSettings struct {
	ParallelDBAndViewCompaction     bool
	DatabaseFragmentationPercentage *json.Number
	DatabaseFragmentationSize       *json.Number
	ViewFragmentationPercentage     *json.Number
	ViewFragmentationSize           *json.Number
	AllowedTimePeriod               *AllowedTimePeriod
}

// AllowedTimePeriod is the time of day auto-compaction may run in.
type AllowedTimePeriod struct {
	FromHour     json.Number
	ToHour       json.Number
	FromMinute   json.Number
	ToMinute     json.Number
	AbortOutside bool
}

// ErrBucketExists is returned when adding a bucket whose name, ignoring
// case, another bucket has.
var ErrBucketExists = errors.New("bucket already exists")

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

	// MaxTTL returns the bucket's maxTTL in seconds, 0 if it has none.
	MaxTTL() uint32

	// Settings returns the settings the bucket keeps and reports only.
	Settings() BucketSettings
}
