package store

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// Document represents one document stored in the vbucket.  For the purposes
// of simplicity of this mock storage engine, this one object is used
// throughout the implementation.
type Document struct {
	VbID         uint
	Key          []byte
	Cas          uint64
	Value        []byte
	Xattrs       map[string][]byte
	SeqNo        uint64
	IsDeleted    bool
	ModifiedTime time.Time
}

// Vbucket represents a single Vbucket worth of documents
type Vbucket struct {
	chrono    *mocktime.Chrono
	lock      sync.Mutex
	documents []*Document
	seqNoIncr uint64
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

	if startSeqNo > endSeqNo {
		return nil, errors.New("start seqno must come before the end seqno")
	}
	if endSeqNo > maxSeqNo {
		return nil, errors.New("end seqno cannot exceed the vbuckets max seqno")
	}

	var docsOut []*Document
	for _, doc := range s.documents {
		if doc.SeqNo >= startSeqNo && doc.SeqNo < endSeqNo {
			docsOut = append(docsOut, copyDocument(doc))
		}
	}

	return docsOut, nil
}

func (s *Vbucket) maxSeqNoLocked() uint64 {
	return s.seqNoIncr - 1
}

func (s *Vbucket) nextSeqNoLocked() uint64 {
	curSeqNo := s.seqNoIncr
	s.seqNoIncr++
	return curSeqNo
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

// addRepDocMutation directly pushes a particular document state to this vbucket.
// It is used by the replicator to push the data directly, ensuring that seqnos
// match between the primary and replica vbuckets.
func (s *Vbucket) addRepDocMutation(doc *Document) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Make sure we aren't somehow getting out of order sequence numbers...
	if doc.SeqNo < s.seqNoIncr {
		log.Printf("unexpected seqno during replica document set")
		return
	}

	// Force the max seqno to the newly added document.
	s.seqNoIncr = doc.SeqNo + 1

	// Create a copy of the document and insert it into our data.
	newDoc := copyDocument(doc)
	s.documents = append(s.documents, newDoc)
}

// compact will compact all of the mutations within a vbucket such that no two
// sequence numbers exist which are for the same document key.
func (s *Vbucket) compact() error {
	// TODO(brett19): Implement support for compaction in case it is needed.
	return errors.New("not implemented")
}
