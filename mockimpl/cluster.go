package mockimpl

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mocktime"
	"github.com/google/uuid"
)

// Cluster represents an instance of a mock cluster
type Cluster struct {
	id              string
	enabledFeatures []ClusterFeature
	numVbuckets     uint
	chrono          *mocktime.Chrono
	replicaLatency  time.Duration

	buckets []*Bucket
	nodes   []*ClusterNode

	currentConfig []byte

	kvInHooks  KvHookManager
	kvOutHooks KvHookManager
	mgmtHooks  MgmtHookManager
}

// NewClusterOptions allows the specification of initial options for a new cluster.
type NewClusterOptions struct {
	Chrono          *mocktime.Chrono
	EnabledFeatures []ClusterFeature
	NumVbuckets     uint
	InitialNode     NewNodeOptions
	ReplicaLatency  time.Duration
}

// NewCluster instantiates a new cluster instance.
func NewCluster(opts NewClusterOptions) (*Cluster, error) {
	if opts.Chrono == nil {
		opts.Chrono = &mocktime.Chrono{}
	}
	if opts.NumVbuckets == 0 {
		opts.NumVbuckets = 1024
	}
	if opts.ReplicaLatency == 0 {
		opts.ReplicaLatency = 50 * time.Millisecond
	}

	cluster := &Cluster{
		id:              uuid.New().String(),
		enabledFeatures: opts.EnabledFeatures,
		numVbuckets:     opts.NumVbuckets,
		chrono:          opts.Chrono,
		replicaLatency:  opts.ReplicaLatency,
		buckets:         nil,
		nodes:           nil,
	}

	// Since it doesn't make sense to have no nodes in a cluster, we force
	// one to be added here at creation time.  Theoretically nothing will break
	// if there are no nodes in the cluster, but this might change in the future.
	cluster.AddNode(opts.InitialNode)

	// I don't really like this, but the default implementations have to be in the
	// same package as us or we end up with a circular dependancy.  Maybe fix it with
	// interfaces later...
	(&kvImplHello{}).Register(&cluster.kvInHooks)
	(&kvImplAuth{}).Register(&cluster.kvInHooks)
	(&kvImplCccp{}).Register(&cluster.kvInHooks)
	(&kvImplCrud{}).Register(&cluster.kvInHooks)
	(&mgmtImplConfig{}).Register(&cluster.mgmtHooks)

	return cluster, nil
}

// ID returns the uuid of this cluster.
func (c *Cluster) ID() string {
	return c.id
}

func (c *Cluster) nodeUuids() []string {
	var out []string
	for _, node := range c.nodes {
		out = append(out, node.ID())
	}
	return out
}

// AddNode will add a new node to a cluster.
func (c *Cluster) AddNode(opts NewNodeOptions) (*ClusterNode, error) {
	node, err := NewClusterNode(c, opts)
	if err != nil {
		return nil, err
	}

	c.nodes = append(c.nodes, node)

	c.updateConfig()
	return node, nil
}

// AddBucket will add a new bucket to a cluster.
func (c *Cluster) AddBucket(opts NewBucketOptions) (*Bucket, error) {
	bucket, err := newBucket(c, opts)
	if err != nil {
		return nil, err
	}

	// Do an initial rebalance for the nodes we currently have
	bucket.UpdateVbMap(c.nodeUuids())

	c.buckets = append(c.buckets, bucket)

	c.updateConfig()
	return bucket, nil
}

// GetBucket will return a specific bucket from the cluster.
func (c *Cluster) GetBucket(name string) *Bucket {
	for _, bucket := range c.buckets {
		if bucket.Name() == name {
			return bucket
		}
	}
	return nil
}

// IsFeatureEnabled will indicate whether this cluster has a specific feature enabled.
func (c *Cluster) IsFeatureEnabled(feature ClusterFeature) bool {
	for _, supportedFeature := range c.enabledFeatures {
		if supportedFeature == feature {
			return true
		}
	}

	return false
}

func (c *Cluster) updateConfig() {
	c.currentConfig = nil
}

// ConnectionString returns the basic non-TLS connection string for this cluster.
func (c *Cluster) ConnectionString() string {
	nodesList := make([]string, 0)
	for _, node := range c.nodes {
		if node.kvService != nil {
			nodesList = append(nodesList,
				fmt.Sprintf("%s:%d", node.kvService.Hostname(), node.kvService.ListenPort()))
		}
	}
	return "couchbase://" + strings.Join(nodesList, ",")
}

// KvInHooks returns the hook manager for incoming kv packets.
func (c *Cluster) KvInHooks() *KvHookManager {
	return &c.kvInHooks
}

// KvOutHooks returns the hook manager for outgoing kv packets.
func (c *Cluster) KvOutHooks() *KvHookManager {
	return &c.kvOutHooks
}

// MgmtHooks returns the hook manager for management requests.
func (c *Cluster) MgmtHooks() *MgmtHookManager {
	return &c.mgmtHooks
}

func (c *Cluster) handleKvPacketIn(source *KvClient, pak *memd.Packet) {
	log.Printf("received kv packet %p CMD:%s", source, pak.Command.Name())
	if c.kvInHooks.Invoke(source, pak) {
		// If we reached the end of the chain, it means nobody replied and we need
		// to default to sending a generic unsupported status code back...
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusUnknownCommand,
		})
		return
	}
}

func (c *Cluster) handleKvPacketOut(source *KvClient, pak *memd.Packet) bool {
	log.Printf("sending kv packet %p CMD:%s %+v", source, pak.Command.Name(), pak)
	if !c.kvOutHooks.Invoke(source, pak) {
		log.Printf("throwing away kv packet %p CMD:%s", source, pak.Command.Name())
		return false
	}
	return true
}

func (c *Cluster) handleMgmtRequest(source *MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received mgmt request %p %+v", source, req)
	return c.mgmtHooks.Invoke(source, req)
}

func (c *Cluster) handleViewRequest(source *ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received view request %p %+v", source, req)
	// TODO(brett19): Implement views request processing
	return nil
}

func (c *Cluster) handleQueryRequest(source *QueryService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received query request %p %+v", source, req)
	// TODO(brett19): Implement query request processing
	return nil
}

func (c *Cluster) handleSearchRequest(source *SearchService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received search request %p %+v", source, req)
	// TODO(brett19): Implement search request processing
	return nil
}

func (c *Cluster) handleAnalyticsRequest(source *AnalyticsService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received analytics request %p %+v", source, req)
	// TODO(brett19): Implement analytics request processing
	return nil
}
