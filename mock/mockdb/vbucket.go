package mockdb

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// Document represents one document stored in the vbucket.  For the purposes
// of simplicity of this mock storage engine, this one object is used
// throughout the implementation.
type Document struct {
	VbID         uint
	CollectionID uint
	Key          []byte
	Value        []byte
	Xattrs       map[string][]byte
	Flags        uint32
	Datatype     uint8
	IsDeleted    bool
	Expiry       time.Time
	LockExpiry   time.Time

	VbUUID       uint64
	Cas          uint64
	SeqNo        uint64
	ModifiedTime time.Time
	RevID        uint64
}

func copyDocument(src *Document) *Document {
	var dst Document

	dst.VbID = src.VbID
	dst.CollectionID = src.CollectionID
	dst.Key = append([]byte{}, src.Key...)
	dst.Flags = src.Flags
	dst.Datatype = src.Datatype
	dst.IsDeleted = src.IsDeleted
	dst.Expiry = src.Expiry
	dst.LockExpiry = src.LockExpiry

	dst.VbUUID = src.VbUUID
	dst.Cas = src.Cas
	dst.SeqNo = src.SeqNo
	dst.ModifiedTime = src.ModifiedTime
	dst.RevID = src.RevID

	dst.Value = append([]byte{}, src.Value...)

	dst.Xattrs = make(map[string][]byte)
	for key, value := range src.Xattrs {
		dst.Xattrs[key] = append([]byte{}, value...)
	}

	return &dst
}

// VbRevData represents an entry in the revision history for this vbucket.
type VbRevData struct {
	VbUUID uint64
	SeqNo  uint64
}

// Vbucket represents a single Vbucket worth of documents
type Vbucket struct {
	chrono         *mocktime.Chrono
	lock           sync.Mutex
	documents      []*Document
	maxSeqNo       uint64
	replicaLatency time.Duration
	persistLatency time.Duration
	revData        []VbRevData
}

type newVbucketOptions struct {
	Chrono         *mocktime.Chrono
	ReplicaLatency time.Duration
	PersistLatency time.Duration
}

func newVbucket(opts newVbucketOptions) (*Vbucket, error) {
	revData := []VbRevData{
		{
			VbUUID: generateNewVbUUID(),
			SeqNo:  0,
		},
	}

	return &Vbucket{
		chrono:         opts.Chrono,
		replicaLatency: opts.ReplicaLatency,
		persistLatency: opts.PersistLatency,
		revData:        revData,
	}, nil
}

func (s *Vbucket) maxSeqNoLocked() uint64 {
	return s.maxSeqNo
}

func (s *Vbucket) nextSeqNoLocked() uint64 {
	s.maxSeqNo++
	return s.maxSeqNo
}

func (s *Vbucket) currentUUIDLocked() uint64 {
	curRevData := s.revData[len(s.revData)-1]
	return curRevData.VbUUID
}

func (s *Vbucket) hasDocExpired(doc *Document) bool {
	// TODO(brett19): Need to emit a delete mutation when a document expires.
	return !doc.Expiry.IsZero() && !s.chrono.Now().Before(doc.Expiry)
}

func (s *Vbucket) findDocLocked(repIdx, collectionID uint, key []byte) *Document {
	// TODO(brett19): Maybe someday we can improve the performance of this by
	// scanning from end-to-start instead of start-to-end...

	// This scans from the start of the array to the end, looking for the last
	// document with the contents we want in it.

	// Calculate when replica becomes visible
	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := s.chrono.Now().Add(-repLatency)

	var foundDoc *Document
	for _, doc := range s.documents {
		if repIdx > 0 && !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}

		if doc.CollectionID == collectionID && bytes.Equal(doc.Key, key) {
			foundDoc = doc
		}
	}

	if foundDoc != nil {
		// Need to COW this.
		foundDoc = copyDocument(foundDoc)

		// We cheat and convert an expired document directly to being deleted.
		if s.hasDocExpired(foundDoc) {
			foundDoc.IsDeleted = true
		}

		// We also cheat and clean this up here...
		if !s.chrono.Now().Before(foundDoc.LockExpiry) {
			foundDoc.LockExpiry = time.Time{}
		}
	}

	return foundDoc
}

// pushDocMutationLocked adds a document mutation to the vbucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) pushDocMutationLocked(doc *Document) *Document {
	newDoc := copyDocument(doc)
	newDoc.VbUUID = s.currentUUIDLocked()
	newDoc.SeqNo = s.nextSeqNoLocked()
	newDoc.ModifiedTime = s.chrono.Now()
	newDoc.RevID++

	s.documents = append(s.documents, newDoc)

	return copyDocument(newDoc)
}

// VbMetaState holds some information about the meta-state of a vbucket.
type VbMetaState struct {
	VbUUID       uint64
	CurrentSeqNo uint64
	PersistSeqNo uint64
}

// CurrentMetaState returns the current sequence numbering information.  Returns
// the vbuuid, seqno and persistSeqno.
func (s *Vbucket) CurrentMetaState(repIdx uint) VbMetaState {
	s.lock.Lock()
	defer s.lock.Unlock()

	curTime := s.chrono.Now()

	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := curTime.Add(-repLatency)

	prsLatency := repLatency + s.persistLatency
	prsVisibleTime := curTime.Add(-prsLatency)

	var currentSeqNo uint64
	var persistSeqNo uint64

	for _, doc := range s.documents {
		if !doc.ModifiedTime.After(repVisibleTime) {
			if doc.SeqNo > currentSeqNo {
				currentSeqNo = doc.SeqNo
			}
		}

		if !doc.ModifiedTime.After(prsVisibleTime) {
			if doc.SeqNo > persistSeqNo {
				persistSeqNo = doc.SeqNo
			}
		}
	}

	return VbMetaState{
		VbUUID:       s.currentUUIDLocked(),
		CurrentSeqNo: currentSeqNo,
		PersistSeqNo: persistSeqNo,
	}
}

// GetAll returns all documents in the vbucket.
func (s *Vbucket) GetAll(repIdx, collectionID uint) ([]*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Calculate when replica becomes visible
	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := s.chrono.Now().Add(-repLatency)

	var docs []*Document
	for _, doc := range s.documents {
		if repIdx > 0 && !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}

		if doc.CollectionID != collectionID {
			continue
		}
		// We cheat and convert an expired document directly to being deleted.
		if s.hasDocExpired(doc) {
			doc.IsDeleted = true
		}

		// We also cheat and clean this up here...
		if !s.chrono.Now().Before(doc.LockExpiry) {
			doc.LockExpiry = time.Time{}
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// Get returns a document in the vbucket by key
func (s *Vbucket) Get(repIdx, collectionID uint, key []byte) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	foundDoc := s.findDocLocked(repIdx, collectionID, key)
	if foundDoc == nil {
		return nil, ErrDocNotFound
	}

	return foundDoc, nil
}

// GetRandom returns a random, not deleted or expired, document in the vbucket
func (s *Vbucket) GetRandom(repIdx, collectionID uint) *Document {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.documents) == 0 {
		return nil
	}

	// Calculate when replica becomes visible
	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := s.chrono.Now().Add(-repLatency)

	var foundDoc *Document
	numDocs := len(s.documents)
	startDocIdx := rand.Intn(numDocs)
	for i := 0; i < numDocs; i++ {
		curr := uint((startDocIdx + i) % numDocs)
		doc := s.documents[curr]

		if !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}

		if doc.CollectionID == collectionID {
			foundDoc = doc
		}

		if foundDoc != nil {
			// Need to COW this.
			foundDoc = copyDocument(foundDoc)

			if s.hasDocExpired(foundDoc) {
				foundDoc = nil
				continue
			}

			// We also cheat and clean this up here...
			if !s.chrono.Now().Before(foundDoc.LockExpiry) {
				foundDoc.LockExpiry = time.Time{}
			}
		}

		if foundDoc != nil {
			break
		}
	}

	return foundDoc
}

// GetAllWithin returns a list of all the mutations that have occurred
// in a vbucket within the bounds of the sequence numbers passed.
// NOTE: There is an assumption that the items returned by this method are in
// ascending seqno order, and that last-modified times are in ascending order
// as well.
func (s *Vbucket) GetAllWithin(repIdx uint, startSeqNo, endSeqNo uint64) ([]*Document, uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	maxSeqNo := s.maxSeqNoLocked()
	if endSeqNo == 0 {
		endSeqNo = maxSeqNo
	}

	if startSeqNo >= endSeqNo {
		return nil, 0, errors.New("start seqno must come before the end seqno")
	}
	if endSeqNo > maxSeqNo {
		return nil, 0, errors.New("end seqno cannot exceed the vbuckets max seqno")
	}

	var docsOut []*Document
	for _, doc := range s.documents {
		if doc.SeqNo > startSeqNo && doc.SeqNo <= endSeqNo {
			docsOut = append(docsOut, copyDocument(doc))
		}
	}

	vbUUID := s.currentUUIDLocked()

	return docsOut, vbUUID, nil
}

// insert stores a document to the vbucket, failing if the specified
// key already exists within the vbucket.
func (s *Vbucket) insert(doc *Document) (*Document, error) {
	return s.update(doc.CollectionID, doc.Key, func(existingDoc *Document) (*Document, error) {
		if existingDoc != nil && !existingDoc.IsDeleted {
			return nil, ErrDocExists
		}

		return doc, nil
	})
}

// UpdateFunc represents a function which can modify the state of a document.
type UpdateFunc func(*Document) (*Document, error)

// update allows a document to be atomically operated upon in the vbucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) update(collectionID uint, key []byte, fn UpdateFunc) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Try to find the document as input to the functor.
	foundDoc := s.findDocLocked(0, collectionID, key)

	// Pass the existing document to the functor and get back the new copy.
	newDoc, err := fn(foundDoc)
	if err != nil {
		return nil, err
	}

	if len(newDoc.Value) > 20*1024*1024 {
		return nil, ErrValueTooBig
	}

	// If no error was returned and no document was returned, ignore the write.
	if newDoc == nil {
		return nil, errors.New("functor did not return a document")
	}

	return s.pushDocMutationLocked(newDoc), nil
}

// Compact will compact all of the mutations within a vbucket such that no two
// sequence numbers exist which are for the same document key.
func (s *Vbucket) Compact() error {
	// We do not currently support performing compaction on a vbucket.  This is
	// fundementally required as the way replicas works means that arbitrary
	// rollbacks are possible.  If a rollback rolls into a compacted section, its
	// possible we would loose mutations which were compacted into later mutations
	// outside the rollback.

	return errors.New("not supported")
}

type vbucketSnapshot struct {
	VbUUID uint64
	SeqNo  uint64
}

// snapshot will return a vbucketSnapshot which can be later used to execute a
// rollback of this vbucket to this particular point in time.
func (s *Vbucket) snapshot() *vbucketSnapshot {
	s.lock.Lock()
	defer s.lock.Unlock()

	return &vbucketSnapshot{
		VbUUID: s.currentUUIDLocked(),
		SeqNo:  s.maxSeqNo,
	}
}

func (s *Vbucket) isSeqNoInHistoryLocked(vbUUID, seqNo uint64) bool {
	foundHistIdx := -1

	for histIdx := len(s.revData) - 1; histIdx >= 0; histIdx-- {
		revData := s.revData[histIdx]
		if revData.VbUUID == vbUUID {
			foundHistIdx = histIdx
			break
		}
	}

	if foundHistIdx == -1 {
		// We did not find this vbuuid inside the history at all.
		return false
	}

	if seqNo < s.revData[foundHistIdx].SeqNo {
		// Somehow this item has a seqno from before this vbuuid started
		// it's history.  This should be impossible, but just in case...
		return false
	}

	if foundHistIdx+1 >= len(s.revData) {
		// This is the last entry in the history, everything past it is fine.
		return true
	}

	if seqNo >= s.revData[foundHistIdx+1].SeqNo {
		// This seqno is later in the history than the vbuuid would indicated.
		// This likely indicates that the mutation came from a revision which
		// has since been rolled back a new history started.
		return false
	}

	// Otherwise we are within the acceptable bounds.
	return true
}

// rollback will rollback this vbucket to a specific seqno (including that seqno).
// It will additionally rollback the history for this vbucket to match.
func (s *Vbucket) rollback(snap *vbucketSnapshot) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isSeqNoInHistoryLocked(snap.VbUUID, snap.SeqNo) {
		return errors.New("snapshot is no longer valid")
	}

	newMutations := make([]*Document, 0, len(s.documents))
	for _, mutation := range s.documents {
		if mutation.SeqNo <= snap.SeqNo {
			newMutations = append(newMutations, mutation)
		}
	}

	s.documents = newMutations
	s.maxSeqNo = snap.SeqNo

	s.revData = append(s.revData, VbRevData{
		VbUUID: 0,
		SeqNo:  s.maxSeqNo,
	})

	return nil
}

// Flush is a basic implementation of this process and simply resets the documents in the vbucket and resets the
// max seq no
func (s *Vbucket) Flush() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.documents = make([]*Document, 0)
	s.revData = []VbRevData{
		{
			VbUUID: generateNewVbUUID(),
			SeqNo:  0,
		},
	}
	s.maxSeqNo = 0
}
