package mockimpl

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// Catches bucket settings, or the cluster's configuration revision, read and
// written without a lock; the race detector reports it.
func TestUpdateBucketDuringKvTraffic(t *testing.T) {
	c := newReplicationConn(t, time.Millisecond, 0)

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			replicas, flush, quota := uint(1), i%2 == 0, uint64(100+i)*1024*1024
			err := c.cluster.UpdateBucket("default", mock.UpdateBucketOptions{
				NumReplicas:  &replicas,
				FlushEnabled: &flush,
				RamQuota:     &quota,
			})
			if err != nil {
				done <- err
				return
			}
		}
	}()
	// The updater stops however the test ends.
	t.Cleanup(func() {
		close(stop)
		if err := <-done; err != nil {
			t.Errorf("failed to update bucket: %v", err)
		}
	})

	durable := &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}
	for i := 0; i < 50; i++ {
		req := &memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte("{}"), Extras: make([]byte, 8)}
		if i%2 == 1 {
			req.DurabilityLevelFrame = durable
		}
		c.mustSucceed(req)
		c.mustSucceed(&memd.Packet{Command: memd.CmdGetClusterConfig})
		c.cluster.ConfigRev()
	}
}

// TestAddBucketNameConflict checks that a bucket whose name another has,
// whatever its case, is refused before its store is made.
func TestAddBucketNameConflict(t *testing.T) {
	cluster, err := NewCluster(mock.NewClusterOptions{InitialNode: mock.NewNodeOptions{}})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	if _, err := cluster.AddBucket(mock.NewBucketOptions{Name: "default", Type: mock.BucketTypeCouchbase}); err != nil {
		t.Fatalf("failed to add bucket: %v", err)
	}

	var logged bytes.Buffer
	log.SetOutput(&logged)
	_, err = cluster.AddBucket(mock.NewBucketOptions{Name: "Default", Type: mock.BucketTypeCouchbase})
	log.SetOutput(os.Stderr)
	if err != mock.ErrBucketExists {
		t.Errorf("expected ErrBucketExists, got %v", err)
	}
	if strings.Contains(logged.String(), "new bucket created") {
		t.Errorf("the refused bucket was made: %s", logged.String())
	}
}

// Catches the bucket config reading the collection manifest's uid while
// a scope is added; the race detector reports it.
func TestBucketConfigDuringManifestChange(t *testing.T) {
	cluster := newBucketsTestCluster(t)
	bucket := cluster.GetBucket("default")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			if _, err := bucket.CollectionManifest().AddScope(fmt.Sprintf("s%d", i)); err != nil {
				t.Errorf("failed to add scope: %v", err)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		readBody(t, bucketsRequest(t, cluster, http.MethodGet, "/default", "", ""))
	}
	<-done
}
