package mockdb

import (
	"errors"
	"log"
	"time"

	"github.com/couchbaselabs/gocaves/mocktime"
)

// Bucket represents a Bucket store
type Bucket struct {
	chrono      *mocktime.Chrono
	vbuckets    [][]*Vbucket
	replicators []*replicator
}

// BucketConfig specifies the configuration for a new Bucket store.
type BucketConfig struct {
	Chrono         *mocktime.Chrono
	NumReplicas    uint
	NumVbuckets    uint
	ReplicaLatency time.Duration
}

// NewBucket will create a new Bucket store.
func NewBucket(config BucketConfig) (*Bucket, error) {
	if config.NumVbuckets < 1 {
		return nil, errors.New("must configure at least 1 vbucket")
	}

	vbReplicas := make([][]*Vbucket, config.NumReplicas+1)
	for repIdx := range vbReplicas {
		vbuckets := make([]*Vbucket, config.NumVbuckets)
		for vbIdx := range vbuckets {
			vbucket, err := newVbucket(vbucketConfig{
				Chrono: config.Chrono,
			})
			if err != nil {
				return nil, err
			}

			vbuckets[vbIdx] = vbucket
		}

		vbReplicas[repIdx] = vbuckets
	}

	var replicators []*replicator
	for repIdx := range vbReplicas {
		if repIdx == 0 {
			// We can't replicate to the first replica, obviously...
			continue
		}

		replicator, err := newReplicator(replicatorConfig{
			Chrono:      config.Chrono,
			SrcVbuckets: vbReplicas[0],
			DstVbuckets: vbReplicas[repIdx],
			Latency:     config.ReplicaLatency,
		})
		if err != nil {
			log.Printf("failed to start replicator: %v", err)
		}

		replicators = append(replicators, replicator)
	}

	bucket := &Bucket{
		chrono:      config.Chrono,
		vbuckets:    vbReplicas,
		replicators: replicators,
	}

	return bucket, nil
}

func (b *Bucket) signalReplicators(vbIdx uint) {
	for _, replicator := range b.replicators {
		replicator.Signal(vbIdx)
	}
}

// Get fetches a document from a particular replica and vbucket index.
func (b *Bucket) Get(repIdx, vbIdx uint, collectionID uint, key []byte) (*Document, error) {
	vbucket := b.GetVbucket(repIdx, vbIdx)
	return vbucket.Get(collectionID, key)
}

// Insert inserts a document into the master replica of a vbucket.
func (b *Bucket) Insert(doc *Document) (*Document, error) {
	vbucket := b.GetVbucket(0, doc.VbID)
	doc, err := vbucket.insert(doc)
	b.signalReplicators(doc.VbID)
	return doc, err
}

// Set stores a document into the master replica of a vbucket.
func (b *Bucket) Set(doc *Document) (*Document, error) {
	vbucket := b.GetVbucket(0, doc.VbID)
	doc, err := vbucket.set(doc)
	b.signalReplicators(doc.VbID)
	return doc, err
}

// Remove removes a document from the master replica of a vbucket.
func (b *Bucket) Remove(vbIdx uint, key []byte) (*Document, error) {
	// Removing a document is explicitly not supported.  See Vbucket::remove
	return nil, errors.New("not supported")
}

// GetVbucket will return the Vbucket object for a particular replica and
// vbucket index within this particular bucket store.
func (b *Bucket) GetVbucket(repIdx, vbIdx uint) *Vbucket {
	if repIdx > uint(len(b.vbuckets)) {
		return nil
	}

	vbuckets := b.vbuckets[repIdx]
	if vbIdx > uint(len(vbuckets)) {
		return nil
	}

	return vbuckets[vbIdx]
}

// Compact will compact all of the vbuckets within this bucket.
// NOTE: If this function bails out due to an error for any reason, it's possible
// that not all buckets will have been compacted.  They are performed in sequence
// from first to last.  Also, we do not compact the replicas, so any mutation
// streams from them are always uncompacted.
func (b *Bucket) Compact() error {
	for _, vbucket := range b.vbuckets[0] {
		if err := vbucket.Compact(); err != nil {
			return err
		}
	}

	return nil
}

// BucketSnapshot represents a snapshot of the bucket at a point in time.  This
// can later be used to rollback the bucket to this point in time.
type BucketSnapshot struct {
	vbuckets []*vbucketSnapshot
}

// Snapshot returns a snapshot of the current state of a Bucket which can be used
// to later rollback to the bucket to that point in time.
func (b *Bucket) Snapshot() *BucketSnapshot {
	primaryVbuckets := b.vbuckets[0]

	snapshots := make([]*vbucketSnapshot, 0, len(primaryVbuckets))
	for _, vbucket := range primaryVbuckets {
		snapshots = append(snapshots, vbucket.snapshot())
	}

	return &BucketSnapshot{
		vbuckets: snapshots,
	}
}

// Rollback will rollback the bucket to a previously snapshotted state.
func (b *Bucket) Rollback(snap *BucketSnapshot) error {
	// Pause all the replicators first
	for _, replicator := range b.replicators {
		replicator.Pause()
	}

	// Rollback all the vbuckets
	for _, vbReplicas := range b.vbuckets {
		for vbIdx, vbucket := range vbReplicas {
			vbucket.rollback(snap.vbuckets[vbIdx])
		}
	}

	// Rollback the replicators
	primaryVbuckets := b.vbuckets[0]
	for _, replicator := range b.replicators {
		for vbIdx := range primaryVbuckets {
			replicator.Rollback(uint(vbIdx), snap.vbuckets[vbIdx].SeqNo)
		}
	}

	// We can now resume all the replicators.
	for _, replicator := range b.replicators {
		replicator.Resume()
	}

	return nil
}
