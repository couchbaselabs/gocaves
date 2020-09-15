package mockdb

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

// MakeRandomCas is the default implementation of CAS generation.  This
// method will create a CAS purely using an RNG.  This is used by Couchbase
// Server < 6.0
func MakeRandomCas() uint64 {
	return rand.Uint64()
}

// MakeTimedCas will generate a CAS value where the current time is included
// as part of the value.  This is used by Couchbase Server >= 6.0.
/*  NOT IMPLEMENTED YET...
func MakeTimedCas() uint64 {
	return 0
}
*/

// Content represents the content of a document in the store.
type Content struct {
	Expiry time.Time
	Value  []byte
	Xattrs map[string][]byte
}

// Document provides access to the information about a document.
type Document struct {
	VbucketID    uint16
	CollectionID uint32
	Key          []byte
	Cas          uint64
	Content      Content
}

type storeDoc struct {
	VbucketID    uint16
	CollectionID uint32
	Key          []byte
	Cas          uint64
	Content      Content
	SeqNo        uint16
}

type storeVb struct {
	UUID     uint64
	MaxSeqNo uint64
}

// Store provides a storage engine for documents.
type Store struct {
	lock      sync.Mutex
	vbuckets  []*storeVb
	documents []*storeDoc

	casGenerator func() uint64
}

// NewStoreOptions provides options for creating a new store.
type NewStoreOptions struct {
	NumVbuckets  uint
	CasGenerator func() uint64
}

// NewStore creates a new store based on the passed options.
func NewStore(opts NewStoreOptions) *Store {
	if opts.CasGenerator == nil {
		opts.CasGenerator = MakeRandomCas
	}

	return &Store{
		casGenerator: opts.CasGenerator,
	}
}

type kvDocCallback func(Document) bool

func (s *Store) forEachLocked(cb kvDocCallback) {
	for _, doc := range s.documents {
		if !cb(Document{
			VbucketID:    doc.VbucketID,
			CollectionID: doc.CollectionID,
			Key:          doc.Key,
			Cas:          doc.Cas,
			Content:      doc.Content,
		}) {
			return
		}
	}
}

// GetAll returns all the documents in the store.
func (s *Store) GetAll() []Document {
	var docs []Document
	s.lock.Lock()
	s.forEachLocked(func(doc Document) bool {
		docs = append(docs, doc)
		return true
	})
	s.lock.Unlock()
	return docs
}

// GetAllInCollection returns all the documents in a collection in the store.
func (s *Store) GetAllInCollection(
	collectionID uint32,
) []Document {
	var docs []Document
	s.lock.Lock()
	s.forEachLocked(func(doc Document) bool {
		if doc.CollectionID == collectionID {
			docs = append(docs, doc)
		}
		return true
	})
	s.lock.Unlock()
	return docs
}

// Get returns a document from the store.
func (s *Store) Get(
	vbID uint16,
	collectionID uint32,
	key []byte,
) (Document, bool) {
	var foundDoc *Document
	s.lock.Lock()
	s.forEachLocked(func(doc Document) bool {
		if doc.VbucketID == vbID {
			if doc.CollectionID == collectionID {
				if bytes.Compare(doc.Key, key) == 0 {
					foundDoc = &doc
					return false
				}
			}
		}
		return true
	})
	s.lock.Unlock()

	if foundDoc == nil {
		return Document{}, false
	}

	return *foundDoc, true
}

// Insert inserts a document
func (s *Store) Insert(
	vbID uint16,
	collectionID uint32,
	key []byte,
	content Content,
) (Document, error) {
	alreadyExists := false
	s.lock.Lock()
	s.forEachLocked(func(doc Document) bool {
		if doc.VbucketID == vbID {
			if doc.CollectionID == collectionID {
				if bytes.Compare(doc.Key, key) == 0 {
					alreadyExists = true
					return false
				}
			}
		}
		return true
	})

	if alreadyExists {
		return Document{}, ErrDocExists
	}

	newDoc := &storeDoc{
		VbucketID:    vbID,
		CollectionID: collectionID,
		Key:          key,
		Cas:          s.casGenerator(),
		Content:      content,
	}
	s.documents = append(s.documents, newDoc)

	s.lock.Unlock()

	return Document{
		VbucketID:    newDoc.VbucketID,
		CollectionID: newDoc.CollectionID,
		Key:          newDoc.Key,
		Cas:          newDoc.Cas,
		Content:      newDoc.Content,
	}, nil
}

// Remove removes a document
func (s *Store) Remove(
	vbID uint16,
	collectionID uint32,
	key []byte,
	content Content,
) error {
	s.lock.Lock()

	foundDocIdx := -1
	for docIdx, doc := range s.documents {
		if doc.VbucketID == vbID {
			if doc.CollectionID == collectionID {
				if bytes.Compare(doc.Key, key) == 0 {
					foundDocIdx = docIdx
					break
				}
			}
		}
	}

	if foundDocIdx >= 0 {
		s.documents = append(s.documents[0:foundDocIdx], s.documents[foundDocIdx+1:]...)
	}

	s.lock.Unlock()

	if foundDocIdx == -1 {
		return ErrDocNotFound
	}

	return nil
}
