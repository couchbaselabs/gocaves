package mockimpl

import (
	"log"

	"github.com/couchbaselabs/gocaves/mock/mockmr"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/google/uuid"
)

// bucketInst represents an instance of a bucketInst
type bucketInst struct {
	id                  string
	cluster             *clusterInst
	name                string
	bucketType          mock.BucketType
	numReplicas         uint
	numVbuckets         uint
	store               *mockdb.Bucket
	configRev           uint
	flushEnabled        bool
	ramQuota            uint64
	replicaIndexEnabled bool
	compressionMode     mock.CompressionMode

	// vbMap is an array for each vbucket, containing an array for
	// each replica, containing the UUID of the node responsible.
	// If a ClusterNode is removed, then it will still be in this map
	// until a rebalance.  We do not keep ClusterNode pointers here
	// directly so we can avoid needing to have a cyclical dependancy.
	vbMap [][]string

	collManifest *mock.CollectionManifest

	viewEngine *mockmr.Engine
}

func newBucket(parent *clusterInst, opts mock.NewBucketOptions) (*bucketInst, error) {
	vbuckets := parent.numVbuckets
	replicas := opts.NumReplicas
	if opts.Type == mock.BucketTypeMemcached {
		vbuckets = 1
		replicas = 0 // This should already be set to 0 by the caller but let's force it.
	}

	// We currently always use a single replica here.  We use this 1 replica for all
	// replicas that are needed, and it is potentially unused if the buckets replica
	// count is 0.
	bucketStore, err := mockdb.NewBucket(mockdb.NewBucketOptions{
		Chrono:         parent.chrono,
		NumReplicas:    1,
		NumVbuckets:    vbuckets,
		ReplicaLatency: parent.replicaLatency,
		PersistLatency: parent.persistLatency,
	})
	if err != nil {
		return nil, err
	}

	bucket := &bucketInst{
		id:                  uuid.New().String(),
		cluster:             parent,
		name:                opts.Name,
		bucketType:          opts.Type,
		numReplicas:         replicas,
		numVbuckets:         vbuckets,
		store:               bucketStore,
		collManifest:        mock.NewCollectionManifest(),
		viewEngine:          mockmr.NewEngine(),
		replicaIndexEnabled: opts.ReplicaIndexEnabled,
		flushEnabled:        opts.FlushEnabled,
		ramQuota:            opts.RamQuota,
		compressionMode:     opts.CompressionMode,
	}

	// Initially set up the vbucket map with nothing in it.
	bucket.UpdateVbMap(nil)

	log.Printf("new bucket created: %s", bucket.Name())
	return bucket, nil
}

// ID returns the uuid of this bucket.
func (b bucketInst) ID() string {
	return b.id
}

// Name returns the name of this bucket
func (b bucketInst) Name() string {
	return b.name
}

// BucketType returns the type of bucket this is.
func (b bucketInst) BucketType() mock.BucketType {
	return b.bucketType
}

// NumReplicas returns the number of configured replicas for this bucket
func (b bucketInst) NumReplicas() uint {
	return b.numReplicas
}

// ConfigRev returns the current configuration revision for this bucket.
func (b bucketInst) ConfigRev() uint {
	return b.configRev
}

// CollectionManifest returns the collection manifest of this bucket.
func (b bucketInst) CollectionManifest() *mock.CollectionManifest {
	return b.collManifest
}

// Store returns the data-store for this bucket.
func (b bucketInst) Store() *mockdb.Bucket {
	return b.store
}

// UpdateVbMap will update the vbmap such that all vbuckets are assigned to the
// specific nodes which are passed in.  Note that this rebalance is guarenteed to
// be very explicit such that vbNode = (vbId % numNode), and replicas are just ++.
func (b *bucketInst) UpdateVbMap(nodeList []string) {
	numVbuckets := b.numVbuckets
	numDataCopies := b.numReplicas + 1

	// Setup the new vb map
	newVbMap := make([][]string, numVbuckets)
	for vbIdx := range newVbMap {
		newVbMap[vbIdx] = make([]string, numDataCopies)
		for repIdx := range newVbMap[vbIdx] {
			newVbMap[vbIdx][repIdx] = ""
		}
	}

	for vbIdx := range newVbMap {
		newVbMap[vbIdx] = make([]string, numDataCopies)
		for repIdx := range newVbMap[vbIdx] {
			if repIdx >= len(nodeList) {
				continue
			}

			nodeIdx := (vbIdx + repIdx) % len(nodeList)
			newVbMap[vbIdx][repIdx] = nodeList[nodeIdx]
		}
	}

	b.vbMap = newVbMap

	b.updateConfig()
}

func (b *bucketInst) updateConfig() {
	b.configRev++
}

// GetVbServerInfo returns the vb nodes, then the vb map, then the ordered list of all nodes
func (b *bucketInst) GetVbServerInfo(reqNode mock.ClusterNode) ([]mock.ClusterNode, [][]int, []mock.ClusterNode) {
	allNodes := b.cluster.nodes

	var nodeList uniqueClusterNodeList

	idxdVbMap := make([][]int, len(b.vbMap))
	for vbIdx, repMap := range b.vbMap {
		idxdVbMap[vbIdx] = make([]int, len(repMap))
		for repIdx, nodeID := range repMap {
			idxdVbMap[vbIdx][repIdx] = nodeList.GetByID(allNodes, nodeID)
		}
	}

	// Grab the KV server list before we add the remaining nodes.
	kvNodes := []mock.ClusterNode(nodeList)

	// Add the remaining nodes for the nodesExt and such.
	for _, node := range allNodes {
		nodeList.GetByID(allNodes, node.ID())
	}

	return kvNodes, idxdVbMap, nodeList
}

func (b *bucketInst) VbucketOwnership(node mock.ClusterNode) []int {
	if b.bucketType == mock.BucketTypeMemcached {
		return []int{0}
	}

	getRepIdx := func(vb []string) int {
		for repIdx, nodeID := range vb {
			if nodeID == node.ID() {
				return repIdx
			}
		}
		return -1
	}

	vbOwnership := make([]int, len(b.vbMap))
	for vbIdx, vb := range b.vbMap {
		vbOwnership[vbIdx] = getRepIdx(vb)
	}
	return vbOwnership
}

func (b *bucketInst) ViewIndexManager() mock.ViewIndexManager {
	return b.viewEngine
}

func (b *bucketInst) Flush() {
	b.Store().Flush()
}

func (b *bucketInst) FlushEnabled() bool {
	return b.flushEnabled
}

func (b *bucketInst) RamQuota() uint64 {
	return b.ramQuota
}

func (b *bucketInst) ReplicaIndexEnabled() bool {
	return b.replicaIndexEnabled
}

func (b *bucketInst) CompressionMode() mock.CompressionMode {
	return b.compressionMode
}

func (b *bucketInst) Update(opts mock.UpdateBucketOptions) error {
	b.ramQuota = opts.RamQuota
	b.flushEnabled = opts.FlushEnabled
	b.replicaIndexEnabled = opts.ReplicaIndexEnabled
	b.numReplicas = opts.NumReplicas

	// TODO: When the store actually does something with num replicas we should probably update it here.

	return nil
}
