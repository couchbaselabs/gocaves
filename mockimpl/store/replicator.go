package store

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/couchbaselabs/gocaves/mocktime"
)

/**
	The implementation of this replicator is intended to be relatively trivial.  Each
	time it wakes up it checks the src vbuckets seqno's and if there are new changes it
	fetches them and checks their modification time to decide whether they should be
	replicated yet.  If so, they are immediately pushed to the destination vbucket.  In
	the case that there are pending changes but they have not reached the replication
	latency yet, it keeps track of the oldest item which will need to be replicated and
	sets a timer for that time, when it will reawake and check again.  If the replicator
	is paused, we simply mark the replicator as disabled so any previously scheduled
	timers don't try and perform replication.  Upon resumption, and any time that the
	replicator is signaled by its parent that there was a change, the main logic is run
	again to replicate anything that needs to be and potentially schedule a new timer.
	This all assumes that the last modified times and seqno's are always ascending and
	takes advantage of the fact that the latency is the same for all documents, meaning
	newer documents are guarenteed to replicate after newer documents, meaning that
	the running timer never needs to be reconfigured.
**/

// replicator defines a replication system which copies data between vbuckets
// based on a specific replication latency.
type replicator struct {
	chrono      *mocktime.Chrono
	srcVbuckets []*Vbucket
	dstVbuckets []*Vbucket
	latency     time.Duration

	lock        sync.Mutex
	disabled    bool
	hasTimerSet bool
	maxSeqNos   []uint64
}

type replicatorConfig struct {
	Chrono      *mocktime.Chrono
	SrcVbuckets []*Vbucket
	DstVbuckets []*Vbucket
	Latency     time.Duration
}

func newReplicator(config replicatorConfig) (*replicator, error) {
	if len(config.SrcVbuckets) != len(config.DstVbuckets) {
		return nil, errors.New("vbucket counts must match")
	}

	maxSeqNos := make([]uint64, len(config.SrcVbuckets))

	return &replicator{
		chrono:      config.Chrono,
		srcVbuckets: config.SrcVbuckets,
		dstVbuckets: config.DstVbuckets,
		latency:     config.Latency,
		maxSeqNos:   maxSeqNos,
	}, nil
}

func (r *replicator) checkVbucketsLocked() {
	if r.disabled {
		return
	}

	curTime := r.chrono.Now()
	var nextReplicateWake time.Time

	for vbIdx := range r.srcVbuckets {
		srcVbucket := r.srcVbuckets[vbIdx]
		dstVbucket := r.dstVbuckets[vbIdx]
		replicatedSeqNo := r.maxSeqNos[vbIdx]

		// Grab the maximum sequence number for this vbucket
		srcMaxSeqNo := srcVbucket.MaxSeqNo()

		// If we've already replicated it, we can immediately continue
		if replicatedSeqNo >= srcMaxSeqNo {
			continue
		}

		docs, err := srcVbucket.GetAllWithin(replicatedSeqNo, srcMaxSeqNo)
		if err != nil || len(docs) == 0 {
			// This would be extremely strange, but let's proceed.
			log.Printf("unexpected empty document list during replication")
			continue
		}

		for _, doc := range docs {
			replicationTime := doc.ModifiedTime.Add(r.latency)
			if curTime.Before(replicationTime) {
				if nextReplicateWake.IsZero() || replicationTime.Before(nextReplicateWake) {
					nextReplicateWake = replicationTime
				}
				break
			}

			dstVbucket.addRepDocMutation(doc)
		}
	}

	if !nextReplicateWake.IsZero() {
		if !r.hasTimerSet {
			r.hasTimerSet = true

			replicateWait := nextReplicateWake.Sub(r.chrono.Now())
			r.chrono.AfterFunc(replicateWait, r.replicationTimerTripped)
		}
	}
}

func (r *replicator) replicationTimerTripped() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.hasTimerSet = false
	r.checkVbucketsLocked()
}

func (r *replicator) Pause() {
	r.lock.Lock()
	defer r.lock.Unlock()

	// When we pause, we set disabled to true to ensure if any of the timers
	// trip, that they are ignored.
	r.disabled = true
}

func (r *replicator) Resume() {
	r.lock.Lock()
	defer r.lock.Unlock()

	// When we resume, we turn on the replicator and then manually trigger
	// a check for any replications that need to happen in case there is no
	// timer running.
	r.disabled = false
	r.checkVbucketsLocked()
}

// Rollback will rollback this replicator to an older point.  It's used during
// bucket rollback to reset the replicator to where it should be.
// NOTE: This method MUST NOT be called unless the replicator is paused.
func (r *replicator) Rollback(vbIdx uint, seqNo uint64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.disabled {
		return errors.New("replicator must be paused to rollback")
	}

	if seqNo < r.maxSeqNos[vbIdx] {
		r.maxSeqNos[vbIdx] = seqNo
	}

	return nil
}

func (r *replicator) Signal(vbIdx uint) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.checkVbucketsLocked()
}
