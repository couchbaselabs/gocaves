package mockdb

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
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

	// InMemory, when set on the document a functor returns, makes the
	// update change only its CAS and lock, in memory, without a mutation,
	// as kv_engine's lock, unlock and a touch that keeps the expiry do. A
	// replica does not see the change, and the next mutation of the key
	// replaces it. It is not stored.
	InMemory bool

	// CasIsHLC reports, on a document read, that kv_engine counts its CAS as
	// a hybrid logical clock. It is not stored.
	CasIsHLC bool

	// SoftDelete, set on a deleted document handed to a mutation, makes the
	// mutation a deletion as DELETE stores one (VBucket::processSoftDelete),
	// whose revision the vbucket counts among those of deleted documents.
	// It is not stored.
	SoftDelete bool

	// SeqNoMacro, when set on a document handed to a mutation, is replaced
	// in its xattrs by the sequence number the mutation gets, in the form
	// kv_engine expands ${Mutation.seqno} to. It is not stored.
	SeqNoMacro []byte

	// SyncWriteInProgress is set on the committed document an update hands its
	// functor when the key has a durable write in progress, so a request that
	// reads before it stores can tell where kv_engine refuses it.
	SyncWriteInProgress bool

	// XattrOrder lists the keys of Xattrs in the order kv_engine's xattr
	// blob holds them. SetXattr and RemoveXattr keep it.
	XattrOrder []string

	// SyncWrite, when set on a document handed to a mutation, makes the
	// mutation a durable write, tracked from the moment it gets its sequence
	// number. It is not stored with the document.
	SyncWrite *SyncWrite

	// A stored durable write is a prepare, as in kv_engine: reads see the
	// committed document before it until it commits at syncEnd, and never if
	// it is aborted then.
	prepare     bool
	syncEnd     time.Time
	syncAborted bool

	// deletedRevID is the revision a deletion leaves as the highest of the
	// deleted documents of its vbucket, or 0 for any other mutation.
	deletedRevID uint64
}

// visibleAt reports whether doc, as stored, can be read at mock time at.
func (doc *Document) visibleAt(at time.Time) bool {
	return !doc.prepare || (!at.Before(doc.syncEnd) && !doc.syncAborted)
}

// SyncWrite is a durable write, tracked by its vbucket as kv_engine's
// durability monitor tracks a prepare, until its response has been sent.
type SyncWrite struct {
	// PersistActive is set when the active copy acknowledges the write only
	// once it has persisted it: majority_and_persist_to_active and
	// persist_to_majority.
	PersistActive bool
	// PersistReplicas is set when replicas acknowledge the write only once
	// they have persisted it: persist_to_majority.
	PersistReplicas bool
	// Replicas is the number of replicas that must acknowledge the write
	// besides the active copy.
	Replicas uint
	// Deadline is the mock time after which the write is aborted, once
	// every earlier write on the vbucket has completed.
	Deadline time.Time
	// Client identifies the connection that made the write. kv_engine
	// completes the writes of a vbucket in sequence-number order, so a
	// connection receives their responses in that order.
	Client any

	vbUUID uint64
	seqNo  uint64
	// end is the first instant the write is complete, and aborted whether it
	// is aborted then. Both follow from the write and the one before it, so
	// they are fixed when the write is stored.
	end     time.Time
	aborted bool
}

// SyncWriteState is the state of a durable write at a given mock time.
type SyncWriteState int

const (
	// SyncWriteUntracked is a write the vbucket does not track: it was not
	// durable, it is in another history than the current one, or it has been
	// answered.
	SyncWriteUntracked SyncWriteState = iota
	SyncWritePending
	SyncWriteCommitted
	SyncWriteAborted
)

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
	dst.XattrOrder = append([]string{}, src.XattrOrder...)

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
	ephemeral      bool
	revData        []VbRevData
	// syncWrites are the durable writes of the current history whose
	// responses are still to be sent, in sequence-number order.
	syncWrites []SyncWrite
	// lastSyncWriteEnd is when the last durable write stored in the current
	// history completes, and lastSyncWriteAborted whether it is aborted.
	lastSyncWriteEnd     time.Time
	lastSyncWriteAborted bool
	// discardedSyncWrites are the writes a flush or rollback discarded
	// before their responses were sent. They keep the responses to a
	// client in sequence-number order.
	discardedSyncWrites []SyncWrite
	// restored holds, by key, the sequence number of a deletion an EP bucket
	// has persisted and dropped from memory that a read brought back
	// (GET_META, or a read of deleted documents).
	restored map[inMemoryKey]uint64
	// inMemory holds, by key, the CAS and lock of the committed mutation of
	// a key as kv_engine changes them in memory only, without a mutation:
	// a lock, an unlock, or a touch that keeps the expiry.
	inMemory map[inMemoryKey]inMemoryState
}

type inMemoryKey struct {
	collectionID uint
	key          string
}

type inMemoryState struct {
	// seqNo is the mutation the state applies to.
	seqNo      uint64
	cas        uint64
	lockExpiry time.Time
}

type newVbucketOptions struct {
	Chrono         *mocktime.Chrono
	ReplicaLatency time.Duration
	PersistLatency time.Duration
	Ephemeral      bool
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
		ephemeral:      opts.Ephemeral,
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
	return !doc.Expiry.IsZero() && !s.chrono.Now().Before(doc.Expiry)
}

// expiredDocument returns doc as kv_engine leaves a document that expires:
// deleted, with no body and only its system xattrs (document_pre_expiry).
func expiredDocument(doc *Document) *Document {
	doc = copyDocument(doc)
	doc.IsDeleted = true
	doc.Value = []byte{}
	doc.Datatype = 0
	for key := range doc.Xattrs {
		if !strings.HasPrefix(key, "_") {
			doc.RemoveXattr(key)
		}
	}
	return doc
}

// expireLocked stores the deletion of doc, a live document of the active copy
// that expired, as kv_engine does when it reads one: a mutation with a new
// CAS and sequence number (VBucket::processExpiredItem). It returns the
// deleted document.
func (s *Vbucket) expireLocked(doc *Document) *Document {
	doc = expiredDocument(doc)
	doc.Cas = GenerateNewCas(s.chrono.Now())
	doc.LockExpiry = time.Time{}
	doc = s.pushDocMutationLocked(doc)
	// kv_engine counts one more than the expired document's revision
	// among those of deleted documents.
	s.documents[len(s.documents)-1].deletedRevID = doc.RevID + 1
	return doc
}

// casIsHLCLocked reports whether kv_engine marks the CAS of doc as a hybrid
// logical clock: once an EP bucket's vbucket has persisted a mutation, its
// HLC epoch is the lowest sequence number of that flush, and every later
// mutation's CAS counts (EPBucket::flushVBucket, Item::toItemInfo). An
// ephemeral bucket counts every CAS.
func (s *Vbucket) casIsHLCLocked(doc *Document, now time.Time) bool {
	if s.ephemeral {
		return true
	}
	if len(s.documents) == 0 {
		return false
	}
	first := s.documents[0]
	return !first.ModifiedTime.Add(s.persistLatency).After(now) && doc.SeqNo >= first.SeqNo
}

// latestLocked returns the stored mutation of a key a copy sees at mock
// time now.
func (s *Vbucket) latestLocked(repIdx, collectionID uint, key []byte, now time.Time) *Document {
	// TODO(brett19): Maybe someday we can improve the performance of this by
	// scanning from end-to-start instead of start-to-end...

	// Calculate when replica becomes visible
	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := now.Add(-repLatency)

	var foundDoc *Document
	for _, doc := range s.documents {
		if repIdx > 0 && !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}
		if !doc.visibleAt(repVisibleTime) {
			continue
		}

		if doc.CollectionID == collectionID && bytes.Equal(doc.Key, key) {
			foundDoc = doc
		}
	}
	return foundDoc
}

// applyInMemoryLocked gives doc, a copy of stored, the CAS and lock the
// active copy holds for it in memory. An entry for an earlier mutation of
// the key is dropped.
func (s *Vbucket) applyInMemoryLocked(repIdx uint, doc, stored *Document) {
	if repIdx != 0 {
		return
	}
	k := inMemoryKey{stored.CollectionID, string(stored.Key)}
	state, ok := s.inMemory[k]
	if !ok {
		return
	}
	if state.seqNo != stored.SeqNo {
		delete(s.inMemory, k)
		return
	}
	doc.Cas, doc.LockExpiry = state.cas, state.lockExpiry
}

func (s *Vbucket) findDocLocked(repIdx, collectionID uint, key []byte) *Document {
	return s.findDocAtLocked(repIdx, collectionID, key, s.chrono.Now(), true)
}

// findDocAtLocked is findDocLocked at mock time now. With expire, a live
// document of the active copy that expired is deleted by a mutation, as
// kv_engine's reads through fetchValidValue and fetchValueForWrite do;
// otherwise it is only reported deleted, as GET_META and the stores that
// replace an expired document see it.
func (s *Vbucket) findDocAtLocked(repIdx, collectionID uint, key []byte, now time.Time, expire bool) *Document {
	foundDoc := s.latestLocked(repIdx, collectionID, key, now)
	if foundDoc != nil {
		stored := foundDoc
		// Need to COW this.
		foundDoc = copyDocument(foundDoc)
		s.applyInMemoryLocked(repIdx, foundDoc, stored)

		// kv_engine expires a live document of the active copy when it reads
		// it, and not a deleted one (VBucket::fetchValidValue). A replica
		// does not see an expired document; the deletion reaches it from the
		// active copy. Nor is a document expired under a durable write in
		// progress.
		if !foundDoc.IsDeleted && !foundDoc.Expiry.IsZero() && !now.Before(foundDoc.Expiry) {
			if expire && repIdx == 0 && !s.syncWriteInProgressLocked(collectionID, key, now) {
				foundDoc = s.expireLocked(foundDoc)
			} else {
				foundDoc.IsDeleted = true
			}
		}

		// An EP bucket drops a deletion from memory once it is persisted, and
		// reads it back from disk with the time of the deletion as its
		// expiry, as VBucket::queueDirty stored it. An ephemeral bucket keeps
		// it in memory, without one.
		if s.deletionOnDiskLocked(stored, now) {
			foundDoc.Expiry = foundDoc.ModifiedTime
		}
		foundDoc.CasIsHLC = s.casIsHLCLocked(foundDoc, now)

		// We also cheat and clean this up here...
		if !now.Before(foundDoc.LockExpiry) {
			foundDoc.LockExpiry = time.Time{}
		}
	}

	return foundDoc
}

// RestoreDeleted brings back to memory a deletion of the key that an EP bucket
// has persisted, as kv_engine does when a read of deleted documents fetches
// it from disk.
func (s *Vbucket) RestoreDeleted(collectionID uint, key []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.restoreLocked(collectionID, key, s.chrono.Now())
}

// deletionOnDiskLocked reports whether stored is a deletion an EP bucket has
// persisted.
func (s *Vbucket) deletionOnDiskLocked(stored *Document, now time.Time) bool {
	return !s.ephemeral && stored.IsDeleted && !stored.ModifiedTime.Add(s.persistLatency).After(now)
}

// restoreLocked brings back to memory a deletion of the key that an EP
// bucket has persisted, as kv_engine does when it fetches it from disk.
func (s *Vbucket) restoreLocked(collectionID uint, key []byte, now time.Time) {
	stored := s.latestLocked(0, collectionID, key, now)
	if stored == nil || !s.deletionOnDiskLocked(stored, now) {
		return
	}
	if s.restored == nil {
		s.restored = make(map[inMemoryKey]uint64)
	}
	s.restored[inMemoryKey{collectionID, string(key)}] = stored.SeqNo
}

// maxDeletedRevIDLocked returns the highest revision kv_engine counts among
// the deleted documents of the vbucket: that of a deletion DELETE stores,
// committed, and one more than that of an expired document
// (HashTable::updateMaxDeletedRevSeqno).
func (s *Vbucket) maxDeletedRevIDLocked(now time.Time) uint64 {
	var maxRevID uint64
	for _, doc := range s.documents {
		if doc.deletedRevID > maxRevID && doc.visibleAt(now) {
			maxRevID = doc.deletedRevID
		}
	}
	return maxRevID
}

// pushDocMutationLocked adds a document mutation to the vbucket.
// NOTE: This must never be called on a replica vbucket.
func (s *Vbucket) pushDocMutationLocked(doc *Document) *Document {
	newDoc := copyDocument(doc)
	newDoc.VbUUID = s.currentUUIDLocked()
	newDoc.SeqNo = s.nextSeqNoLocked()
	newDoc.ModifiedTime = s.chrono.Now()
	newDoc.RevID++
	if doc.SoftDelete {
		newDoc.deletedRevID = newDoc.RevID
	}
	// A committed mutation supersedes what memory held for the key; a
	// prepare does not until it commits.
	if doc.SyncWrite == nil {
		delete(s.inMemory, inMemoryKey{newDoc.CollectionID, string(newDoc.Key)})
	}
	// kv_engine expands the macro in place of padding of the same size, so
	// the xattr keeps its position.
	if doc.SeqNoMacro != nil {
		seqNo := []byte(fmt.Sprintf(`"0x%016x"`, newDoc.SeqNo))
		for key, value := range newDoc.Xattrs {
			newDoc.Xattrs[key] = bytes.ReplaceAll(value, doc.SeqNoMacro, seqNo)
		}
	}

	s.documents = append(s.documents, newDoc)

	if doc.SyncWrite != nil {
		w := *doc.SyncWrite
		w.seqNo = newDoc.SeqNo
		w.vbUUID = newDoc.VbUUID
		w.end, w.aborted = s.syncWriteEndLocked(&w, newDoc.ModifiedTime, s.lastSyncWriteEnd, s.lastSyncWriteAborted)
		s.lastSyncWriteEnd, s.lastSyncWriteAborted = w.end, w.aborted
		s.syncWrites = append(s.syncWrites, w)
		newDoc.prepare, newDoc.syncEnd, newDoc.syncAborted = true, w.end, w.aborted
	}

	return copyDocument(newDoc)
}

// LastSeqNoForUUID returns the last sequence number of an earlier history
// of the vbucket, the one with vbUUID: where the history after it starts. It
// reports false for the latest history and for a UUID the vbucket never had,
// as kv_engine's FailoverTable::getLastSeqnoForUUID does.
func (s *Vbucket) LastSeqNoForUUID(vbUUID uint64) (uint64, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i := 0; i+1 < len(s.revData); i++ {
		if s.revData[i].VbUUID == vbUUID {
			return s.revData[i+1].SeqNo, true
		}
	}
	return 0, false
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

// SyncWriteStateAt returns the state at mock time at of the durable write with
// sequence number seqNo in the history vbUUID.
func (s *Vbucket) SyncWriteStateAt(vbUUID, seqNo uint64, at time.Time) SyncWriteState {
	s.lock.Lock()
	defer s.lock.Unlock()

	if vbUUID != s.currentUUIDLocked() {
		return SyncWriteUntracked
	}
	i, found := slices.BinarySearchFunc(s.syncWrites, seqNo, func(w SyncWrite, seqNo uint64) int {
		return cmp.Compare(w.seqNo, seqNo)
	})
	if !found {
		return SyncWriteUntracked
	}
	switch w := &s.syncWrites[i]; {
	case at.Before(w.end):
		return SyncWritePending
	case w.aborted:
		return SyncWriteAborted
	default:
		return SyncWriteCommitted
	}
}

// syncWriteEndLocked returns the first instant w, stored at stored, is
// complete, and whether it is aborted, given that the write before it on the
// vbucket is complete from prevEnd. kv_engine expires a write only after its
// deadline, so an abort is complete a nanosecond after it. kv_engine completes
// the writes of a vbucket in sequence-number order: a write that meets its
// level waits for the writes before it, and one past its deadline is aborted
// only once it is the first incomplete write. A write behind an aborted one is
// aborted with it if its own deadline has passed, as removeExpired aborts
// every expired write at the front of the queue.
func (s *Vbucket) syncWriteEndLocked(w *SyncWrite, stored, prevEnd time.Time, prevAborted bool) (time.Time, bool) {
	if prevAborted && w.Deadline.Before(prevEnd) {
		return prevEnd, true
	}

	// Replica r receives a mutation r replica latencies after the active
	// copy, and each copy persists it a persist latency after receiving it.
	durable := stored
	if w.PersistActive {
		durable = durable.Add(s.persistLatency)
	}
	if w.Replicas > 0 {
		replicated := stored.Add(time.Duration(w.Replicas) * s.replicaLatency)
		if w.PersistReplicas {
			replicated = replicated.Add(s.persistLatency)
		}
		if replicated.After(durable) {
			durable = replicated
		}
	}

	end, limit := durable, w.Deadline
	if end.Before(prevEnd) {
		end = prevEnd
	}
	if limit.Before(prevEnd) {
		limit = prevEnd
	}
	if end.After(limit) {
		return limit.Add(time.Nanosecond), true
	}
	return end, false
}

// MarkSyncWriteReplied records that the response to the durable write with
// sequence number seqNo in the history vbUUID has been sent, or will not be,
// and stops tracking it.
func (s *Vbucket) MarkSyncWriteReplied(vbUUID, seqNo uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// slices.DeleteFunc clears the vacated tail, so no client stays
	// reachable through the backing arrays.
	answered := func(w SyncWrite) bool { return w.vbUUID == vbUUID && w.seqNo == seqNo }
	s.discardedSyncWrites = slices.DeleteFunc(s.discardedSyncWrites, answered)
	s.syncWrites = slices.DeleteFunc(s.syncWrites, answered)
}

// EarlierSyncWritesReplied reports whether every durable write that the same
// client made on this vbucket before the one with sequence number seqNo in the
// history vbUUID has had its response sent. The writes a flush or rollback
// discarded come before those of the current history.
func (s *Vbucket) EarlierSyncWritesReplied(vbUUID, seqNo uint64, client any) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, writes := range [][]SyncWrite{s.discardedSyncWrites, s.syncWrites} {
		for _, w := range writes {
			if w.vbUUID == vbUUID && w.seqNo == seqNo {
				return true
			}
			if w.Client == client {
				return false
			}
		}
	}
	return true
}

// discardSyncWritesLocked ends the durable writes of the current history,
// keeping them only to order their responses.
func (s *Vbucket) discardSyncWritesLocked() {
	s.discardedSyncWrites = append(s.discardedSyncWrites, s.syncWrites...)
	s.syncWrites = nil
	s.lastSyncWriteEnd, s.lastSyncWriteAborted = time.Time{}, false
}

// GetAll returns all documents in the vbucket.
func (s *Vbucket) GetAll(repIdx, collectionID uint) ([]*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Calculate when replica becomes visible
	repLatency := time.Duration(repIdx) * s.replicaLatency
	repVisibleTime := s.chrono.Now().Add(-repLatency)

	var visible []*Document
	latest := make(map[string]*Document)
	for _, doc := range s.documents {
		if repIdx > 0 && !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}
		if !doc.visibleAt(repVisibleTime) {
			continue
		}

		if doc.CollectionID != collectionID {
			continue
		}
		visible = append(visible, doc)
		latest[string(doc.Key)] = doc
	}

	var docs []*Document
	for _, doc := range visible {
		stored := doc
		if !doc.IsDeleted && s.hasDocExpired(doc) {
			doc = expiredDocument(doc)
		} else {
			doc = copyDocument(doc)
		}
		if latest[string(stored.Key)] == stored {
			s.applyInMemoryLocked(repIdx, doc, stored)
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

// GetMeta returns a document of the active copy by key as kv_engine's
// VBucket::getMetaData reads it: an expired document is reported deleted,
// as it was stored, and its deletion is not stored.
func (s *Vbucket) GetMeta(collectionID uint, key []byte) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	now := s.chrono.Now()
	s.restoreLocked(collectionID, key, now)
	foundDoc := s.findDocAtLocked(0, collectionID, key, now, false)
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

	// The document is reported as the active copy holds it in memory, an
	// expired one too: kv_engine skips only deleted, temporary, non-resident
	// and pending values (HashTable::getRandomKeyFromSlot).
	now := s.chrono.Now()
	if !s.hasItemsLocked(collectionID, now) {
		return nil
	}
	repVisibleTime := now.Add(-time.Duration(repIdx) * s.replicaLatency)
	latest := make(map[string]*Document)
	var keys []string
	for _, doc := range s.documents {
		if doc.CollectionID != collectionID || !doc.visibleAt(repVisibleTime) {
			continue
		}
		if repIdx > 0 && !doc.ModifiedTime.Before(repVisibleTime) {
			continue
		}
		if _, ok := latest[string(doc.Key)]; !ok {
			keys = append(keys, string(doc.Key))
		}
		latest[string(doc.Key)] = doc
	}
	if len(keys) == 0 {
		return nil
	}
	start := rand.Intn(len(keys))
	for i := range keys {
		stored := latest[keys[(start+i)%len(keys)]]
		if stored.IsDeleted {
			continue
		}
		foundDoc := copyDocument(stored)
		s.applyInMemoryLocked(repIdx, foundDoc, stored)
		if !now.Before(foundDoc.LockExpiry) {
			foundDoc.LockExpiry = time.Time{}
		}
		return foundDoc
	}

	return nil
}

// hasItemsLocked reports whether the item count of a collection kv_engine
// checks before it looks for a random document is not 0
// (KVBucket::getRandomKey). An EP bucket counts the committed documents it has
// persisted live (FlushAccounting::StatisticsUpdate::insert); an ephemeral one
// counts them as it stores them.
func (s *Vbucket) hasItemsLocked(collectionID uint, now time.Time) bool {
	counted := now
	if !s.ephemeral {
		counted = now.Add(-s.persistLatency)
	}
	latest := make(map[string]*Document)
	for _, doc := range s.documents {
		if doc.CollectionID != collectionID || doc.ModifiedTime.After(counted) || !doc.visibleAt(counted) {
			continue
		}
		latest[string(doc.Key)] = doc
	}
	for _, doc := range latest {
		if !doc.IsDeleted {
			return true
		}
	}
	return false
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
	}, pendingFirst, false)
}

// UpdateFunc represents a function which can modify the state of a document.
type UpdateFunc func(*Document) (*Document, error)

// syncWriteInProgressLocked reports whether the latest mutation of a key is a
// durable write that has not completed.
func (s *Vbucket) syncWriteInProgressLocked(collectionID uint, key []byte, now time.Time) bool {
	for i := len(s.documents) - 1; i >= 0; i-- {
		doc := s.documents[i]
		if doc.CollectionID == collectionID && bytes.Equal(doc.Key, key) {
			return doc.prepare && now.Before(doc.syncEnd)
		}
	}
	return false
}

// pendingCheck is when a write to a key with a durable write in progress is
// refused with ErrSyncWriteInProgress.
type pendingCheck int

const (
	// pendingFirst refuses it before the functor runs.
	pendingFirst pendingCheck = iota
	// pendingIfFound refuses it before the functor runs when a committed
	// document exists, and leaves the functor to refuse a missing one.
	pendingIfFound
	// pendingAtStore refuses it when the functor returns a document to
	// store.
	pendingAtStore
)

// update allows a document to be atomically operated upon in the vbucket.
// A key with a durable write in progress cannot be written; check says when
// that is found out.
// NOTE: This must never be called on a replica vbucket.
//
// With expire, a document that expired is deleted by a mutation before fn
// sees it, as the kv_engine reads that precede the store do. Without, fn
// sees it deleted and the store replaces it, as VBucket::set, add and
// replace do.
func (s *Vbucket) update(collectionID uint, key []byte, fn UpdateFunc, check pendingCheck, expire bool) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Try to find the document as input to the functor.
	// One instant for both, so a prepare that commits meanwhile is either
	// in progress or the document read.
	now := s.chrono.Now()
	foundDoc := s.findDocAtLocked(0, collectionID, key, now, expire)

	inProgress := s.syncWriteInProgressLocked(collectionID, key, now)
	if foundDoc != nil {
		foundDoc.SyncWriteInProgress = inProgress
	}
	found := foundDoc != nil && !foundDoc.IsDeleted
	if inProgress && (check == pendingFirst || check == pendingIfFound && found) {
		return nil, ErrSyncWriteInProgress
	}

	// Pass the existing document to the functor and get back the new copy.
	newDoc, err := fn(foundDoc)
	if err != nil {
		return nil, err
	}
	if inProgress {
		return nil, ErrSyncWriteInProgress
	}

	if len(newDoc.Value) > 20*1024*1024 {
		return nil, ErrValueTooBig
	}

	// If no error was returned and no document was returned, ignore the write.
	if newDoc == nil {
		return nil, errors.New("functor did not return a document")
	}
	if newDoc.InMemory {
		k := inMemoryKey{collectionID, string(key)}
		stored := s.latestLocked(0, collectionID, key, now)
		if newDoc.Cas == stored.Cas && newDoc.LockExpiry.IsZero() {
			delete(s.inMemory, k)
		} else {
			if s.inMemory == nil {
				s.inMemory = make(map[inMemoryKey]inMemoryState)
			}
			s.inMemory[k] = inMemoryState{stored.SeqNo, newDoc.Cas, newDoc.LockExpiry}
		}
		return s.findDocAtLocked(0, collectionID, key, now, false), nil
	}

	// The revision continues from the document the store replaces, deleted
	// or not, while kv_engine holds it in memory. A new key starts after the
	// highest revision of the deleted documents of the vbucket
	// (VBucket::updateRevSeqNoOfNewStoredValue), as does a key whose
	// persisted deletion an EP bucket has dropped from memory.
	stored := s.latestLocked(0, collectionID, key, now)
	if foundDoc != nil && !(s.deletionOnDiskLocked(stored, now) && s.restored[inMemoryKey{collectionID, string(key)}] != stored.SeqNo) {
		newDoc.RevID = foundDoc.RevID
	} else {
		newDoc.RevID = s.maxDeletedRevIDLocked(now)
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

	// Each later branch starts at the sequence number its rollback kept. A
	// sequence number past any of them was discarded by that rollback.
	for _, later := range s.revData[foundHistIdx+1:] {
		if seqNo > later.SeqNo {
			return false
		}
	}
	return true
}

// rollbackLocked will rollback this vbucket to a specific seqno (including that
// seqno). It will additionally rollback the history for this vbucket to match.
// The snapshot must be in the vbucket's history.
func (s *Vbucket) rollbackLocked(snap *vbucketSnapshot) {
	newMutations := make([]*Document, 0, len(s.documents))
	for _, mutation := range s.documents {
		if mutation.SeqNo <= snap.SeqNo {
			newMutations = append(newMutations, mutation)
		}
	}

	s.documents = newMutations
	s.inMemory = nil
	s.restored = nil
	s.maxSeqNo = snap.SeqNo
	s.discardSyncWritesLocked()

	// Every branch of history gets its own UUID, as a failover entry does
	// on a server, so a mutation is identified by its UUID and sequence
	// number across any number of rollbacks.
	s.revData = append(s.revData, VbRevData{
		VbUUID: generateNewVbUUID(),
		SeqNo:  s.maxSeqNo,
	})
}

// Flush is a basic implementation of this process and simply resets the documents in the vbucket and resets the
// max seq no
func (s *Vbucket) Flush() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.documents = make([]*Document, 0)
	s.inMemory = nil
	s.restored = nil
	s.discardSyncWritesLocked()
	s.revData = []VbRevData{
		{
			VbUUID: generateNewVbUUID(),
			SeqNo:  0,
		},
	}
	s.maxSeqNo = 0
}

// SetXattr stores an xattr as kv_engine's xattr::Blob::set
// (xattr/blob.cc@v7.0.5) does: a value of the same size replaces the old one
// in place, and any other value is appended after the others.
func (d *Document) SetXattr(key string, value []byte) {
	if old, ok := d.Xattrs[key]; ok && len(old) == len(value) {
		d.Xattrs[key] = value
		return
	}
	d.RemoveXattr(key)
	if d.Xattrs == nil {
		d.Xattrs = make(map[string][]byte)
	}
	d.Xattrs[key] = value
	d.XattrOrder = append(d.XattrOrder, key)
}

// RemoveXattr removes an xattr.
func (d *Document) RemoveXattr(key string) {
	delete(d.Xattrs, key)
	for i, k := range d.XattrOrder {
		if k == key {
			d.XattrOrder = append(d.XattrOrder[:i:i], d.XattrOrder[i+1:]...)
			return
		}
	}
}

// XattrKeys returns the keys of the xattrs in the order kv_engine's xattr
// blob holds them.
func (d *Document) XattrKeys() []string {
	keys := make([]string, 0, len(d.Xattrs))
	for _, key := range d.XattrOrder {
		if _, ok := d.Xattrs[key]; ok {
			keys = append(keys, key)
		}
	}
	return keys
}
