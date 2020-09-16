package store

import (
	"errors"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// Bucket represents a Bucket store
type Bucket struct {
	chrono   *mocktime.Chrono
	vbuckets [][]*Vbucket
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

	bucket := &Bucket{
		chrono:   config.Chrono,
		vbuckets: vbReplicas,
	}

	return bucket, nil
}

// Get fetches a document from a particular replica and vbucket index.
func (b *Bucket) Get(repIdx, vbIdx uint, key []byte) (*Document, error) {
	vbucket := b.GetVbucket(repIdx, vbIdx)
	return vbucket.Get(key)
}

// Insert inserts a document into the master replica of a vbucket.
func (b *Bucket) Insert(doc *Document) (*Document, error) {
	vbucket := b.GetVbucket(0, doc.VbID)
	return vbucket.insert(doc)
}

// Set stores a document into the master replica of a vbucket.
func (b *Bucket) Set(doc *Document) (*Document, error) {
	vbucket := b.GetVbucket(0, doc.VbID)
	return vbucket.set(doc)
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
