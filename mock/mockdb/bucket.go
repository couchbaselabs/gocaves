package mockdb

import (
	"errors"
	"math/rand"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// Bucket represents a Bucket store
type Bucket struct {
	chrono   *mocktime.Chrono
	vbuckets []*Vbucket
}

// NewBucketOptions specifies the configuration for a new Bucket store.
type NewBucketOptions struct {
	Chrono         *mocktime.Chrono
	NumReplicas    uint
	NumVbuckets    uint
	ReplicaLatency time.Duration
	PersistLatency time.Duration
}

// NewBucket will create a new Bucket store.
func NewBucket(opts NewBucketOptions) (*Bucket, error) {
	if opts.NumVbuckets < 1 {
		return nil, errors.New("must configure at least 1 vbucket")
	}

	vbuckets := make([]*Vbucket, opts.NumVbuckets)
	for vbIdx := range vbuckets {
		vbucket, err := newVbucket(newVbucketOptions{
			Chrono:         opts.Chrono,
			ReplicaLatency: opts.ReplicaLatency,
			PersistLatency: opts.PersistLatency,
		})
		if err != nil {
			return nil, err
		}

		vbuckets[vbIdx] = vbucket
	}

	bucket := &Bucket{
		chrono:   opts.Chrono,
		vbuckets: vbuckets,
	}

	return bucket, nil
}

// Chrono returns the chrono responsible for this bucket.
func (b *Bucket) Chrono() *mocktime.Chrono {
	return b.chrono
}

// GetAll fetches all documents from a particular replica.
func (b *Bucket) GetAll(repIdx, collectionID uint) ([]*Document, error) {
	var alldocs []*Document
	for _, vb := range b.vbuckets {
		docs, err := vb.GetAll(repIdx, collectionID)
		if err != nil {
			return nil, err
		}

		alldocs = append(alldocs, docs...)
	}

	return alldocs, nil
}

// Get fetches a document from a particular replica and vbucket index.
func (b *Bucket) Get(repIdx, vbIdx uint, collectionID uint, key []byte) (*Document, error) {
	vbucket := b.GetVbucket(vbIdx)
	if vbucket == nil {
		return nil, errors.New("invalid vbucket")
	}

	return vbucket.Get(repIdx, collectionID, key)
}

// GetRandom fetches a random document from a particular replica and vbucket index.
func (b *Bucket) GetRandom(repIdx, collectionID uint) (*Document, error) {
	max := len(b.vbuckets)
	start := uint(rand.Intn(max))
	curr := start
	for {
		vbucket := b.GetVbucket(curr)
		if vbucket == nil {
			continue
		}

		found := vbucket.GetRandom(repIdx, collectionID)
		if found != nil {
			return found, nil
		}

		curr++
		if curr == uint(max) {
			curr = 0
		}

		if curr == start {
			break
		}
	}

	return nil, ErrDocNotFound
}

// Insert inserts a document into the master replica of a vbucket.
func (b *Bucket) Insert(doc *Document) (*Document, error) {
	vbucket := b.GetVbucket(doc.VbID)
	if vbucket == nil {
		return nil, errors.New("invalid vbucket")
	}

	doc, err := vbucket.insert(doc)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// Update allows a document to be atomically operated upon in the bucket.
func (b *Bucket) Update(vbID, collectionID uint, key []byte, fn UpdateFunc) (*Document, error) {
	vbucket := b.GetVbucket(vbID)
	if vbucket == nil {
		return nil, errors.New("invalid vbucket")
	}

	doc, err := vbucket.update(collectionID, key, fn)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// Remove removes a document from the master replica of a vbucket.
func (b *Bucket) Remove(vbIdx uint, key []byte) (*Document, error) {
	// Removing a document is explicitly not supported.  See Vbucket::remove
	return nil, errors.New("not supported")
}

// GetVbucket will return the Vbucket object for a particular replica and
// vbucket index within this particular bucket store.
func (b *Bucket) GetVbucket(vbIdx uint) *Vbucket {
	if vbIdx > uint(len(b.vbuckets)) {
		return nil
	}

	return b.vbuckets[vbIdx]
}

// ForEachVBucket iterates over all the vbuckets in a bucket and runs the provided function.
func (b *Bucket) ForEachVBucket(callback func(vbIdx int, vbucket *Vbucket)) {
	for vbIdx, vbucket := range b.vbuckets {
		callback(vbIdx, vbucket)
	}
}

// Compact will compact all of the vbuckets within this bucket.  This is not
// yet supported.  See Vbucket::Compact for details on why.
func (b *Bucket) Compact() error {
	return errors.New("not supported")
}

// BucketSnapshot represents a snapshot of the bucket at a point in time.  This
// can later be used to rollback the bucket to this point in time.
type BucketSnapshot struct {
	vbuckets []*vbucketSnapshot
}

// Snapshot returns a snapshot of the current state of a Bucket which can be used
// to later rollback to the bucket to that point in time.
func (b *Bucket) Snapshot() *BucketSnapshot {
	snapshots := make([]*vbucketSnapshot, 0, len(b.vbuckets))
	for _, vbucket := range b.vbuckets {
		snapshots = append(snapshots, vbucket.snapshot())
	}

	return &BucketSnapshot{
		vbuckets: snapshots,
	}
}

// Rollback will rollback the bucket to a previously snapshotted state.
func (b *Bucket) Rollback(snap *BucketSnapshot) error {
	// Rollback all the vbuckets
	for vbIdx, vbucket := range b.vbuckets {
		err := vbucket.rollback(snap.vbuckets[vbIdx])
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) Flush() {
	for _, vbucket := range b.vbuckets {
		vbucket.Flush()
	}
}
