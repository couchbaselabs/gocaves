package mock

import (
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// NewClusterOptions allows the specification of initial options for a new cluster.
type NewClusterOptions struct {
	Chrono         *mocktime.Chrono
	NumVbuckets    uint
	InitialNode    NewNodeOptions
	ReplicaLatency time.Duration
	PersistLatency time.Duration
}

// Cluster represents an instance of a mock cluster
type Cluster interface {
	// ID returns the uuid of this cluster.
	ID() string

	// ConfigRev returns the current configuration revision for this cluster.
	ConfigRev() uint

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

	// KvInHooks returns the hook manager for incoming kv packets.
	KvInHooks() KvHookManager

	// KvOutHooks returns the hook manager for outgoing kv packets.
	KvOutHooks() KvHookManager

	// MgmtHooks returns the hook manager for management requests.
	MgmtHooks() MgmtHookManager

	// Chrono returns the chrono object in use by the cluster.
	Chrono() *mocktime.Chrono
}
