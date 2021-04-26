package mockimpl

import (
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/hooks"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/svcimpls"
	"github.com/couchbaselabs/gocaves/mock/mocktime"
	"github.com/google/uuid"
)

// clusterInst represents an instance of a mock cluster
type clusterInst struct {
	id             string
	numVbuckets    uint
	chrono         *mocktime.Chrono
	replicaLatency time.Duration
	persistLatency time.Duration
	tlsConfig      *tls.Config
	configRev      uint

	configWatcherLock sync.Mutex
	configWatchers    []mock.ConfigWatcher

	buckets []*bucketInst
	nodes   []*clusterNodeInst

	auth *mockauth.Engine

	analyticsHooks hooks.AnalyticsHookManager
	kvInHooks      hooks.KvHookManager
	kvOutHooks     hooks.KvHookManager
	mgmtHooks      hooks.MgmtHookManager
	queryHooks     hooks.QueryHookManager
	searchHooks    hooks.SearchHookManager
	viewHooks      hooks.ViewHookManager
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
	if opts.PersistLatency == 0 {
		opts.PersistLatency = 100 * time.Millisecond
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
		id:             uuid.New().String(),
		numVbuckets:    opts.NumVbuckets,
		chrono:         opts.Chrono,
		replicaLatency: opts.ReplicaLatency,
		persistLatency: opts.PersistLatency,
		buckets:        nil,
		nodes:          nil,
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
		auth: mockauth.NewEngine(),
	}

	// Since it doesn't make sense to have no nodes in a cluster, we force
	// one to be added here at creation time.  Theoretically nothing will break
	// if there are no nodes in the cluster, but this might change in the future.
	_, err := cluster.AddNode(opts.InitialNode)
	if err != nil {
		return nil, err
	}

	// I don't really like this, but the default implementations have to be in the
	// same package as us or we end up with a circular dependancy.  Maybe fix it with
	// interfaces later...
	svcimpls.Register(svcimpls.RegisterOptions{
		AnalyticsHooks: &cluster.analyticsHooks,
		KvInHooks:      &cluster.kvInHooks,
		KvOutHooks:     &cluster.kvOutHooks,
		MgmtHooks:      &cluster.mgmtHooks,
		QueryHooks:     &cluster.queryHooks,
		SearchHooks:    &cluster.searchHooks,
		ViewHooks:      &cluster.viewHooks,
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

// GetAllBuckets will return all buckets from the cluster.
func (c *clusterInst) GetAllBuckets() []mock.Bucket {
	var buckets []mock.Bucket
	for _, bucket := range c.buckets {
		buckets = append(buckets, bucket)
	}
	return buckets
}

// ConfigRev returns the current configuration revision for this cluster.
func (c *clusterInst) ConfigRev() uint {
	return c.configRev
}

func (c *clusterInst) updateConfig() {
	c.configRev++
	c.configWatcherLock.Lock()
	watchers := c.configWatchers
	c.configWatcherLock.Unlock()

	for _, w := range watchers {
		w.OnNewConfig(c.configRev)
	}
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

// MgmtHosts returns a list of non-TLS mgmt endpoints for this cluster.
func (c *clusterInst) MgmtAddrs() []string {
	nodesList := make([]string, 0)
	for _, node := range c.nodes {
		if node.mgmtService != nil {
			nodesList = append(nodesList,
				fmt.Sprintf("http://%s:%d", node.mgmtService.Hostname(), node.mgmtService.ListenPort()))
		}
	}
	return nodesList
}

func (c *clusterInst) Chrono() *mocktime.Chrono {
	return c.chrono
}

// KvInHooks returns the hook manager for incoming kv packets.
func (c *clusterInst) KvInHooks() mock.KvHookManager {
	return &c.kvInHooks
}

// KvOutHooks returns the hook manager for outgoing kv packets.
func (c *clusterInst) KvOutHooks() mock.KvHookManager {
	return &c.kvOutHooks
}

// MgmtHooks returns the hook manager for management requests.
func (c *clusterInst) MgmtHooks() mock.MgmtHookManager {
	return &c.mgmtHooks
}

func (c *clusterInst) Users() mock.UserManager {
	return c.auth
}

func (c *clusterInst) AddConfigWatcher(watcher mock.ConfigWatcher) {
	c.configWatcherLock.Lock()
	c.configWatchers = append(c.configWatchers, watcher)
	c.configWatcherLock.Unlock()
}

func (c *clusterInst) RemoveConfigWatcher(watcher mock.ConfigWatcher) {
	c.configWatcherLock.Lock()
	var idx int
	for i, w := range c.configWatchers {
		if w == watcher {
			idx = i
		}
	}

	if idx == len(c.configWatchers) {
		c.configWatchers = c.configWatchers[:idx]
	} else {
		c.configWatchers = append(c.configWatchers[:idx], c.configWatchers[idx+1:]...)
	}
	c.configWatcherLock.Unlock()
}

func (c *clusterInst) handleKvPacketIn(source *kvClient, pak *memd.Packet) {
	log.Printf("received kv packet %p CMD:%s", source, pak.Command.Name())
	if c.kvInHooks.Invoke(source, pak) {
		// If we reached the end of the chain, it means nobody replied and we need
		// to default to sending a generic unsupported status code back...
		err := source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusUnknownCommand,
		})
		if err != nil {
			log.Printf("failed to write unknown command packet: %s", err)
		}
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
	return c.viewHooks.Invoke(source, req)
}

func (c *clusterInst) handleQueryRequest(source *queryService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received query request %p %+v", source, req)
	return c.queryHooks.Invoke(source, req)
}

func (c *clusterInst) handleSearchRequest(source *searchService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received search request %p %+v\n\n\n\n", source, req)
	return c.searchHooks.Invoke(source, req)
}

func (c *clusterInst) handleAnalyticsRequest(source *analyticsService, req *mock.HTTPRequest) *mock.HTTPResponse {
	log.Printf("received analytics request %p %+v", source, req)
	return c.analyticsHooks.Invoke(source, req)
}
