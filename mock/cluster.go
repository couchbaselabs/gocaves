package mock

import (
	"time"

	"github.com/couchbaselabs/gocaves/mocktime"
)

// NewClusterOptions allows the specification of initial options for a new cluster.
type NewClusterOptions struct {
	Chrono          *mocktime.Chrono
	EnabledFeatures []ClusterFeature
	NumVbuckets     uint
	InitialNode     NewNodeOptions
	ReplicaLatency  time.Duration
}

// Cluster represents an instance of a mock cluster
type Cluster interface {
	// ID returns the uuid of this cluster.
	ID() string

	// AddNode will add a new node to a cluster.
	AddNode(opts NewNodeOptions) (ClusterNode, error)

	// AddBucket will add a new bucket to a cluster.
	AddBucket(opts NewBucketOptions) (Bucket, error)

	// Nodes returns a list of all the nodes in this cluster.
	Nodes() []ClusterNode

	// GetBucket will return a specific bucket from the cluster.
	GetBucket(name string) Bucket

	// ConnectionString returns the basic non-TLS connection string for this cluster.
	ConnectionString() string

	// TODO(brett19): Remove the totally hackery which is our hooks managers not being self-dependant...

	// KvInHooks returns the hook manager for incoming kv packets.
	KvInHooks() interface{}

	// KvOutHooks returns the hook manager for outgoing kv packets.
	KvOutHooks() interface{}

	// MgmtHooks returns the hook manager for management requests.
	MgmtHooks() interface{}
}
