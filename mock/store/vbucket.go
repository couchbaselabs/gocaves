package store

import (
	"bytes"
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// Document represents one document stored in the vbucket.  For the purposes
// of simplicity of this mock storage engine, this one object is used
// throughout the implementation.
type Document struct {
	VbID      uint
	Key       []byte
	Value     []byte
	Xattrs    map[string][]byte
	IsDeleted bool

	Cas          uint64
	SeqNo        uint64
	ModifiedTime time.Time
}

// Vbucket represents a single Vbucket worth of documents
type Vbucket struct {
	chrono        *mocktime.Chrono
	lock          sync.Mutex
	documents     []*Document
	maxSeqNo      uint64
	compactionRev uint64
}

type vbucketConfig struct {
	Chrono *mocktime.Chrono
}

func newVbucket(config vbucketConfig) (*Vbucket, error) {
	return &Vbucket{
		chrono: config.Chrono,
	}, nil
}

func copyDocument(src *Document) *Document {
	var dst Document

	dst.Key = append([]byte{}, src.Key...)
	dst.Cas = src.Cas
	dst.SeqNo = src.SeqNo
	dst.Value = append([]byte{}, src.Value...)
	dst.ModifiedTime = src.ModifiedTime

	dst.Xattrs = make(map[string][]byte)
	for key, value := range src.Xattrs {
		dst.Xattrs[key] = append([]byte{}, value...)
	}

	return &dst
}

// MaxSeqNo returns the current maximum sequence number in
// this particular vbucket.
func (s *Vbucket) MaxSeqNo() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.maxSeqNoLocked()
}

// Get returns a document in the vbucket by key
func (s *Vbucket) Get(key []byte) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	foundDoc := s.findDocLocked(key)
	if foundDoc == nil {
		return nil, errors.New("document not found")
	}

	return copyDocument(foundDoc), nil
}

// GetAllWithin returns a list of all the mutations that have occurred
// in a vbucket within the bounds of the sequence numbers passed.
// NOTE: There is an assumption that the items returned by this method are in
// ascending seqno order, and that last-modified times are in ascending order
// as well.
func (s *Vbucket) GetAllWithin(startSeqNo, endSeqNo uint64) ([]*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	maxSeqNo := s.maxSeqNoLocked()
	if endSeqNo == 0 {
		endSeqNo = maxSeqNo
	}

	if startSeqNo >= endSeqNo {
		return nil, errors.New("start seqno must come before the end seqno")
	}
	if endSeqNo > maxSeqNo {
		return nil, errors.New("end seqno cannot exceed the vbuckets max seqno")
	}

	var docsOut []*Document
	for _, doc := range s.documents {
		if doc.SeqNo > startSeqNo && doc.SeqNo <= endSeqNo {
			docsOut = append(docsOut, copyDocument(doc))
		}
	}

	return docsOut, nil
}

func (s *Vbucket) maxSeqNoLocked() uint64 {
	return s.maxSeqNo
}

func (s *Vbucket) nextSeqNoLocked() uint64 {
	s.maxSeqNo++
	return s.maxSeqNo
}

func (s *Vbucket) findDocLocked(key []byte) *Document {
	// TODO(brett19): Maybe someday we can improve the performance of this by
	// scanning from end-to-start instead of start-to-end...

	// This scans from the start of the array to the end, looking for the last
	// document with the contents we want in it.

	var foundDoc *Document
	for _, doc := range s.documents {
		if bytes.Compare(doc.Key, key) == 0 {
			foundDoc = doc
		}
	}

	return foundDoc
}

// pushDocMutationLocked adds a document mutation to the vbucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) pushDocMutationLocked(doc *Document) *Document {
	newDoc := copyDocument(doc)
	newDoc.Cas = generateNewCas()
	newDoc.SeqNo = s.nextSeqNoLocked()
	newDoc.ModifiedTime = s.chrono.Now()

	s.documents = append(s.documents, newDoc)

	return copyDocument(newDoc)
}

// insert stores a document to the vbucket, failing if the specified
// key already exists within the vbucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) insert(doc *Document) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	foundDoc := s.findDocLocked(doc.Key)
	if foundDoc != nil {
		return nil, errors.New("doc already exists")
	}

	return s.pushDocMutationLocked(doc), nil
}

// set stores a document to the bucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) set(doc *Document) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// We scan for an existing document first.  Checking the CAS based on
	// whether we found one or not.  We don't need it other than that though.
	foundDoc := s.findDocLocked(doc.Key)
	if foundDoc != nil {
		if doc.Cas != 0 && foundDoc.Cas != doc.Cas {
			return nil, errors.New("cas mismatch")
		}
	} else {
		if doc.Cas != 0 {
			return nil, errors.New("doc not found")
		}
	}

	return s.pushDocMutationLocked(doc), nil
}

func (s *Vbucket) remove(key []byte) (*Document, error) {
	// Removing a document is explicitly not supported.  Instead a document should be
	// modified such that its IsDeleted field is true.  This is part of the overall
	// archicture of the storage system, and IsDeleted items will be compacted away
	// at some future point in time if appropirate.

	return nil, errors.New("not supported")
}

// addRepDocMutation directly pushes a particular document state to this vbucket.
// It is used by the replicator to push the data directly, ensuring that seqnos
// match between the primary and replica vbuckets.
func (s *Vbucket) addRepDocMutation(doc *Document) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Make sure we aren't somehow getting out of order sequence numbers...
	if doc.SeqNo <= s.maxSeqNo {
		log.Printf("unexpected seqno during replica document set")
		return
	}

	// Force the max seqno to the newly added document.
	s.maxSeqNo = doc.SeqNo

	// Create a copy of the document and insert it into our data.
	newDoc := copyDocument(doc)
	s.documents = append(s.documents, newDoc)
}

// Compact will compact all of the mutations within a vbucket such that no two
// sequence numbers exist which are for the same document key.
func (s *Vbucket) Compact() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Increment the compaction revision to avoid having people use snapshots from
	// before the compaction, which will likely lead to problems...
	s.compactionRev++

	// We take advantage of the fact that strings and []byte's are equivalent in Go.
	// It is specifically allowed by their spec to convert between them, and non-UTF8
	// keys will still be stored and compared correctly:
	keyMap := make(map[string]*Document)

	// Pull all the documents into the map, going oldest to newest.  Duplicate documents
	// for the same key will only end up in the map once, with the most recent version.
	for _, doc := range s.documents {
		keyMap[string(doc.Key)] = doc
	}

	// Let's build out an array again
	docs := make([]*Document, 0, len(keyMap))
	for _, doc := range keyMap {
		docs = append(docs, doc)
	}

	// Now we just need to re-sort the array such that the seqno's are in order like
	// we expect that they should be...
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].SeqNo < docs[j].SeqNo
	})

	// And swap out the documents array!  Note that the maxseqno never changes as part
	// of this swap (but the minseqno might change), since we always take the newest
	// versions of the documents.
	s.documents = docs

	return nil
}

type vbucketSnapshot struct {
	CompactionRev uint64
	SeqNo         uint64
}

// snapshot will return a vbucketSnapshot which can be later used to execute a
// rollback of this vbucket to this particular point in time.
func (s *Vbucket) snapshot() *vbucketSnapshot {
	s.lock.Lock()
	defer s.lock.Unlock()

	return &vbucketSnapshot{
		CompactionRev: s.compactionRev,
		SeqNo:         s.maxSeqNo,
	}
}

// rollback will rollback this vbucket to a specific seqno (including that seqno)
// NOTE: This is not safe to call directly since replicators can be running which
// will blow up if they are not kept in sync.  Additionally, it is not safe to
// rollback a vbucket which has been compacted, since the maxseqno will get broken
// without it being manually adjusted to the rollback point.  This is because rollback
// calculates a maxSeqNo (which is important for replication to know what still needs to
// be replicated).  However on the master side of things, the maxSeqNo must be set exactly
// to the value it was rolled back to, otherwise lower seqNo's might get reused in error.
// if they were compacted away.  The vbucketSnapshot system prevents this.
// Use bucket::RollbackVb instead.
func (s *Vbucket) rollback(snap *vbucketSnapshot) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if snap.CompactionRev != s.compactionRev {
		return errors.New("cannot rollback after a compaction with a snapshot from before")
	}

	var maxSeqNo uint64

	newMutations := make([]*Document, 0, len(s.documents))
	for _, mutation := range s.documents {
		if mutation.SeqNo <= snap.SeqNo {
			newMutations = append(newMutations, mutation)
			if mutation.SeqNo > maxSeqNo {
				maxSeqNo = mutation.SeqNo
			}
		}
	}

	s.documents = newMutations
	s.maxSeqNo = maxSeqNo

	return nil
}
