package svcimpls

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"github.com/couchbaselabs/gocaves/mock/mocktime"
)

// replyClient records the packets written to it. It has no selected bucket,
// and its cluster has no buckets.
type replyClient struct {
	lock    sync.Mutex
	written []*memd.Packet
}

type emptyCluster struct{ mock.Cluster }

func (emptyCluster) GetAllBuckets() []mock.Bucket { return nil }

type emptyClusterNode struct{ mock.ClusterNode }

func (emptyClusterNode) Cluster() mock.Cluster { return emptyCluster{} }

type emptyClusterService struct{ mock.KvService }

func (emptyClusterService) Node() mock.ClusterNode { return emptyClusterNode{} }

func (c *replyClient) LocalAddr() net.Addr                                 { return nil }
func (c *replyClient) RemoteAddr() net.Addr                                { return nil }
func (c *replyClient) IsTLS() bool                                         { return false }
func (c *replyClient) Source() mock.KvService                              { return emptyClusterService{} }
func (c *replyClient) ScramServer() *scramserver.ScramServer               { return nil }
func (c *replyClient) SetAuthenticatedUserName(userName string)            {}
func (c *replyClient) AuthenticatedUserName() string                       { return "" }
func (c *replyClient) CheckAuthenticated(mockauth.Permission, uint32) bool { return true }
func (c *replyClient) SetSelectedBucketName(bucketName string)             {}
func (c *replyClient) SelectedBucketName() string                          { return "" }
func (c *replyClient) SelectedBucket() mock.Bucket                         { return nil }
func (c *replyClient) SetFeatures(features []memd.HelloFeature)            {}
func (c *replyClient) HasFeature(feature memd.HelloFeature) bool           { return false }
func (c *replyClient) Closed() bool                                        { return false }
func (c *replyClient) RunDetached(fn func())                               { fn() }
func (c *replyClient) Close() error                                        { return nil }
func (c *replyClient) WritePacket(pak *memd.Packet) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.written = append(c.written, pak)
	return nil
}

func (c *replyClient) writtenPackets() []*memd.Packet {
	c.lock.Lock()
	defer c.lock.Unlock()
	return append([]*memd.Packet{}, c.written...)
}

// A durable write whose bucket was removed after it was applied is answered
// ambiguous, after the client's earlier durable write on the vbucket.
func TestDurableReplyAfterBucketRemovalFollowsEarlierReply(t *testing.T) {
	chrono := &mocktime.Chrono{}
	store, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1, ReplicaLatency: time.Hour})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	source := &replyClient{}
	var docs []*mockdb.Document
	for _, key := range []string{"earlier", "later"} {
		doc, err := store.Insert(&mockdb.Document{
			VbID:      0,
			Key:       []byte(key),
			Value:     []byte("{}"),
			SyncWrite: &mockdb.SyncWrite{Replicas: 1, Deadline: chrono.Now().Add(time.Hour), Client: source},
		})
		if err != nil {
			t.Fatalf("failed to insert %s: %v", key, err)
		}
		docs = append(docs, doc)
	}
	earlier, later := docs[0], docs[1]
	req := &memd.Packet{
		Command:              memd.CmdSet,
		Opaque:               7,
		DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		(&kvImplCrud{}).writeMutationReply(source, store, req, later.VbUUID, later.SeqNo, &memd.Packet{Status: memd.StatusSuccess}, time.Now())
	}()
	time.Sleep(50 * time.Millisecond)
	if written := source.writtenPackets(); len(written) != 0 {
		t.Fatalf("expected no reply before the earlier write's, got %+v", written)
	}
	store.GetVbucket(0).MarkSyncWriteReplied(earlier.VbUUID, earlier.SeqNo)
	<-done
	if written := source.writtenPackets(); len(written) != 1 || written[0].Status != memd.StatusSyncWriteAmbiguous {
		t.Fatalf("expected one sync_write_ambiguous reply, got %+v", written)
	}
}

// singleVbucketBucket is a bucket with one vbucket, as a memcached bucket has.
// Only the methods the durability wait uses are implemented.
type singleVbucketBucket struct {
	mock.Bucket
	store *mockdb.Bucket
}

func (b *singleVbucketBucket) Store() *mockdb.Bucket { return b.store }
func (b *singleVbucketBucket) NumReplicas() uint     { return 1 }

// newDurableStore creates a one-vbucket store with one replica, and stores a
// majority durable write that times out after timeout of mock time.
func newDurableStore(t *testing.T, replicaLatency, timeout time.Duration) (*mocktime.Chrono, *mockdb.Bucket, *mockdb.Document) {
	t.Helper()

	chrono := &mocktime.Chrono{}
	store, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumReplicas: 1, NumVbuckets: 1, ReplicaLatency: replicaLatency})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	doc, err := store.Insert(&mockdb.Document{
		VbID:      0,
		Key:       []byte("key"),
		Value:     []byte("{}"),
		SyncWrite: &mockdb.SyncWrite{Replicas: 1, Deadline: chrono.Now().Add(timeout)},
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	return chrono, store, doc
}

func TestDurabilityWaitEndedDuringSnapshotIsNotDurable(t *testing.T) {
	_, store, doc := newDurableStore(t, 0, time.Second)

	// The wait ends after its first check, while the vbucket that would show
	// the mutation replicated is being read.
	checks := 0
	ended := func() bool {
		checks++
		return checks > 1
	}
	if waitForDurability(store, 0, doc.VbUUID, doc.SeqNo, ended) {
		t.Errorf("expected a wait that ended while the vbucket was read not to be durable")
	}
}

// A time travel after the deadline, before the poll that reads the copies as
// of the deadline, is not progress made by the deadline.
func TestDurabilityWaitIgnoresTimeTravelAfterDeadline(t *testing.T) {
	chrono, store, doc := newDurableStore(t, time.Second, 5*time.Millisecond)

	deadline := chrono.Now().Add(5 * time.Millisecond)
	travelled := false
	ended := func() bool {
		if !travelled && chrono.Now().After(deadline) {
			chrono.TimeTravel(2 * time.Second)
			travelled = true
		}
		return false
	}
	if waitForDurability(store, 0, doc.VbUUID, doc.SeqNo, ended) {
		t.Errorf("expected a write replicated only after a later time travel not to be durable by its deadline")
	}
	if !travelled {
		t.Fatalf("the wait ended before the deadline passed, so no time travel happened")
	}
}

// The timeout runs on the mock clock, as replication does: a time travel past
// both the deadline and the replication does not make the write durable.
func TestDurabilityWaitTimesOutAcrossTimeTravel(t *testing.T) {
	chrono, store, doc := newDurableStore(t, time.Second, 10*time.Millisecond)

	travelled := false
	ended := func() bool {
		if !travelled {
			chrono.TimeTravel(2 * time.Second)
			travelled = true
		}
		return false
	}
	if waitForDurability(store, 0, doc.VbUUID, doc.SeqNo, ended) {
		t.Errorf("expected a write whose timeout passed before its replica received it not to be durable")
	}
}

// A request without a timeout frame gets kv_engine's 30-second default.
func TestSyncWriteDefaultTimeout(t *testing.T) {
	chrono := &mocktime.Chrono{}
	store, err := mockdb.NewBucket(mockdb.NewBucketOptions{Chrono: chrono, NumVbuckets: 1})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	before := chrono.Now()
	w := syncWrite(nil, &memd.Packet{DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}}, &singleVbucketBucket{store: store})
	after := chrono.Now()

	if w.Deadline.Before(before.Add(30*time.Second)) || w.Deadline.After(after.Add(30*time.Second)) {
		t.Errorf("expected a deadline 30s from now, got %v from %v", w.Deadline.Sub(before), before)
	}
}
