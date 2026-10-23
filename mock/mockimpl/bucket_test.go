package mockimpl

import (
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
			err := c.cluster.(*clusterInst).UpdateBucket("default", mock.UpdateBucketOptions{
				NumReplicas:  1,
				FlushEnabled: i%2 == 0,
				RamQuota:     uint64(100+i) * 1024 * 1024,
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
