package mockimpl

import (
	"log"

	"github.com/couchbaselabs/gocaves/mockimpl/store"
	"github.com/google/uuid"
)

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
}

// Bucket represents an instance of a Bucket
type Bucket struct {
	id          string
	cluster     *Cluster
	name        string
	bucketType  BucketType
	numReplicas uint
	numVbuckets uint
	store       *store.Bucket
	configRev   uint

	// vbMap is an array for each vbucket, containing an array for
	// each replica, containing the UUID of the node responsible.
	// If a ClusterNode is removed, then it will still be in this map
	// until a rebalance.  We do not keep ClusterNode pointers here
	// directly so we can avoid needing to have a cyclical dependancy.
	vbMap [][]string

	collManifest CollectionManifest
}

func newBucket(parent *Cluster, opts NewBucketOptions) (*Bucket, error) {
	// We currently always use a single replica here.  We use this 1 replica for all
	// replicas that are needed, and it is potentially unused if the buckets replica
	// count is 0.
	bucketStore, err := store.NewBucket(store.BucketConfig{
		Chrono:         parent.chrono,
		NumReplicas:    1,
		NumVbuckets:    parent.numVbuckets,
		ReplicaLatency: parent.replicaLatency,
	})
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{
		id:          uuid.New().String(),
		cluster:     parent,
		name:        opts.Name,
		bucketType:  opts.Type,
		numReplicas: opts.NumReplicas,
		numVbuckets: parent.numVbuckets,
		store:       bucketStore,
	}

	// Initially set up the vbucket map with nothing in it.
	bucket.UpdateVbMap(nil)

	log.Printf("new bucket created")
	return bucket, nil
}

// ID returns the uuid of this bucket.
func (b Bucket) ID() string {
	return b.id
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

// CollectionManifest returns the collection manifest of this bucket.
func (b Bucket) CollectionManifest() CollectionManifest {
	return b.collManifest
}

// UpdateVbMap will update the vbmap such that all vbuckets are assigned to the
// specific nodes which are passed in.  Note that this rebalance is guarenteed to
// be very explicit such that vbNode = (vbId % numNode), and replicas are just ++.
func (b *Bucket) UpdateVbMap(nodeList []string) {
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

func (b *Bucket) updateConfig() {
	b.configRev++

	// This is just for testing
	b.GetConfig(nil)
}

// GetVbServerInfo returns the vb nodes, then the vb map, then the ordered list of all nodes
func (b *Bucket) GetVbServerInfo(reqNode *ClusterNode) ([]*ClusterNode, [][]int, []*ClusterNode) {
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
	kvNodes := []*ClusterNode(nodeList)

	// Add the remaining nodes for the nodesExt and such.
	for _, node := range allNodes {
		nodeList.GetByID(allNodes, node.ID())
	}

	return kvNodes, idxdVbMap, nodeList
}
