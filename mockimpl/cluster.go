package mockimpl

import (
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/svcimpls"
	"github.com/couchbaselabs/gocaves/mocktime"
	"github.com/google/uuid"
)

// clusterInst represents an instance of a mock cluster
type clusterInst struct {
	id              string
	enabledFeatures []mock.ClusterFeature
	numVbuckets     uint
	chrono          *mocktime.Chrono
	replicaLatency  time.Duration
	tlsConfig       *tls.Config

	buckets []*bucketInst
	nodes   []*clusterNodeInst

	kvInHooks  hooks.KvHookManager
	kvOutHooks hooks.KvHookManager
	mgmtHooks  hooks.MgmtHookManager
}

// NewCluster instantiates a new cluster instance.
func NewCluster(opts mock.NewClusterOptions) (mock.Cluster, error) {
	if opts.Chrono == nil {
		opts.Chrono = &mocktime.Chrono{}
	}
	if opts.NumVbuckets == 0 {
		opts.NumVbuckets = 1024
	}
	if opts.ReplicaLatency == 0 {
		opts.ReplicaLatency = 50 * time.Millisecond
	}

	// TODO(brett19): Improve cluster/node certificate setup.
	// We Need to generate these dynamically, provide accessors so each node
	// can have its own individual certificates and such that we can generate
	// client-certificate authentication test certificates.  This should
	// probably be wrapped in its own testable package or something.
	certPem := []byte(`-----BEGIN CERTIFICATE-----
	MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
	DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
	EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
	7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
	5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
	BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
	NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
	Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
	6MF9+Yw1Yy0t
	-----END CERTIFICATE-----`)
	keyPem := []byte(`-----BEGIN EC PRIVATE KEY-----
	MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
	AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
	EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
	-----END EC PRIVATE KEY-----`)
	cert, _ := tls.X509KeyPair(certPem, keyPem)

	cluster := &clusterInst{
		id:              uuid.New().String(),
		enabledFeatures: opts.EnabledFeatures,
		numVbuckets:     opts.NumVbuckets,
		chrono:          opts.Chrono,
		replicaLatency:  opts.ReplicaLatency,
		buckets:         nil,
		nodes:           nil,
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
	}

	// Since it doesn't make sense to have no nodes in a cluster, we force
	// one to be added here at creation time.  Theoretically nothing will break
	// if there are no nodes in the cluster, but this might change in the future.
	cluster.AddNode(opts.InitialNode)

	// I don't really like this, but the default implementations have to be in the
	// same package as us or we end up with a circular dependancy.  Maybe fix it with
	// interfaces later...
	svcimpls.Register(svcimpls.RegisterOptions{
		KvInHooks:  &cluster.kvInHooks,
		KvOutHooks: &cluster.kvOutHooks,
		MgmtHooks:  &cluster.mgmtHooks,
	})

	return cluster, nil
}

// ID returns the uuid of this cluster.
func (c *clusterInst) ID() string {
	return c.id
}

func (c *clusterInst) Nodes() []mock.ClusterNode {
	nodes := make([]mock.ClusterNode, len(c.nodes))
	for nodeIdx, node := range c.nodes {
		nodes[nodeIdx] = node
	}
	return nodes
}

func (c *clusterInst) nodeUuids() []string {
	var out []string
	for _, node := range c.nodes {
		out = append(out, node.ID())
	}
	return out
}

// AddNode will add a new node to a cluster.
func (c *clusterInst) AddNode(opts mock.NewNodeOptions) (mock.ClusterNode, error) {
	node, err := newClusterNode(c, opts)
	if err != nil {
		return nil, err
	}

	c.nodes = append(c.nodes, node)

	c.updateConfig()
	return node, nil
}

// AddBucket will add a new bucket to a cluster.
func (c *clusterInst) AddBucket(opts mock.NewBucketOptions) (mock.Bucket, error) {
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
func (c *clusterInst) GetBucket(name string) mock.Bucket {
	for _, bucket := range c.buckets {
		if bucket.Name() == name {
			return bucket
		}
	}
	return nil
}

// IsFeatureEnabled will indicate whether this cluster has a specific feature enabled.
func (c *clusterInst) IsFeatureEnabled(feature mock.ClusterFeature) bool {
	for _, supportedFeature := range c.enabledFeatures {
		if supportedFeature == feature {
			return true
		}
	}

	return false
}

func (c *clusterInst) updateConfig() {
}

// ConnectionString returns the basic non-TLS connection string for this cluster.
func (c *clusterInst) ConnectionString() string {
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
func (c *clusterInst) KvInHooks() interface{} {
	return &c.kvInHooks
}

// KvOutHooks returns the hook manager for outgoing kv packets.
func (c *clusterInst) KvOutHooks() interface{} {
	return &c.kvOutHooks
}

// MgmtHooks returns the hook manager for management requests.
func (c *clusterInst) MgmtHooks() interface{} {
	return &c.mgmtHooks
}

func (c *clusterInst) handleKvPacketIn(source *kvClient, pak *memd.Packet) {
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

func (c *clusterInst) handleKvPacketOut(source *kvClient, pak *memd.Packet) bool {
	log.Printf("sending kv packet %p CMD:%s %+v", source, pak.Command.Name(), pak)
	if !c.kvOutHooks.Invoke(source, pak) {
		log.Printf("throwing away kv packet %p CMD:%s", source, pak.Command.Name())
		return false
	}
	return true
}

func (c *clusterInst) handleMgmtRequest(source *mgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received mgmt request %p %+v", source, req)
	return c.mgmtHooks.Invoke(source, req)
}

func (c *clusterInst) handleViewRequest(source *viewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received view request %p %+v", source, req)
	// TODO(brett19): Implement views request processing
	return nil
}

func (c *clusterInst) handleQueryRequest(source *queryService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received query request %p %+v", source, req)
	// TODO(brett19): Implement query request processing
	return nil
}

func (c *clusterInst) handleSearchRequest(source *searchService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received search request %p %+v", source, req)
	// TODO(brett19): Implement search request processing
	return nil
}

func (c *clusterInst) handleAnalyticsRequest(source *analyticsService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received analytics request %p %+v", source, req)
	// TODO(brett19): Implement analytics request processing
	return nil
}
