package mockimpl

import (
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// Catches the hook chain changed and read without a lock; the race detector
// reports it. The durable writes reply from a goroutine of their own, so the
// chain is also read off the connection's goroutine.
func TestHooksChangeDuringKvTraffic(t *testing.T) {
	for _, test := range []struct {
		name  string
		hooks func(mock.Cluster) mock.KvHookManager
	}{
		{name: "in", hooks: mock.Cluster.KvInHooks},
		{name: "out", hooks: mock.Cluster.KvOutHooks},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newReplicationConn(t, time.Millisecond, 0)

			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					select {
					case <-stop:
						return
					default:
					}
					child := test.hooks(c.cluster).Child()
					child.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
						next()
					})
					child.Destroy()
				}
			}()
			// The hook changer stops however the test ends.
			t.Cleanup(func() {
				close(stop)
				<-done
			})

			durable := &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority}
			for i := 0; i < 50; i++ {
				req := &memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Value: []byte("{}"), Extras: make([]byte, 8)}
				if i%2 == 1 {
					req.DurabilityLevelFrame = durable
				}
				c.mustSucceed(req)
			}
		})
	}
}
