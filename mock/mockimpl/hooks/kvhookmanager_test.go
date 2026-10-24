package hooks

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mockauth"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock"
	tmock "github.com/stretchr/testify/mock"
)

type fakeKvClient struct {
	tmock.Mock
}

func (c *fakeKvClient) LocalAddr() net.Addr                       { return &net.IPAddr{} }
func (c *fakeKvClient) RemoteAddr() net.Addr                      { return &net.IPAddr{} }
func (c *fakeKvClient) IsTLS() bool                               { return false }
func (c *fakeKvClient) Source() mock.KvService                    { return nil }
func (c *fakeKvClient) ScramServer() *scramserver.ScramServer     { return nil }
func (c *fakeKvClient) SetAuthenticatedUserName(userName string)  {}
func (c *fakeKvClient) AuthenticatedUserName() string             { return "" }
func (c *fakeKvClient) SetSelectedBucketName(bucketName string)   {}
func (c *fakeKvClient) SelectedBucketName() string                { return "" }
func (c *fakeKvClient) SelectedBucket() mock.Bucket               { return nil }
func (c *fakeKvClient) SetFeatures(features []memd.HelloFeature)  {}
func (c *fakeKvClient) HasFeature(feature memd.HelloFeature) bool { return false }
func (c *fakeKvClient) WritePacket(pak *memd.Packet) error        { return nil }
func (c *fakeKvClient) Closed() bool                              { return false }
func (c *fakeKvClient) RunDetached(fn func())                     { fn() }
func (c *fakeKvClient) Close() error                              { return nil }
func (c *fakeKvClient) CheckAuthenticated(permission mockauth.Permission, collectionID uint32) bool {
	return true
}

func TestKvHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := mock.KvClient(&fakeKvClient{})
	fakePacket := &memd.Packet{}

	var hooks KvHookManager
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		hookInvokes = append(hookInvokes, 1)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		hookInvokes = append(hookInvokes, 2)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		hookInvokes = append(hookInvokes, 3)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})

	hooks.Invoke(fakeSource, fakePacket)

	if len(hookInvokes) != 3 {
		t.Fatalf("wrong number of invocations")
	}
	if hookInvokes[0] != 3 {
		t.Fatalf("wrong invocation order")
	}
	if hookInvokes[1] != 2 {
		t.Fatalf("wrong invocation order")
	}
	if hookInvokes[2] != 1 {
		t.Fatalf("wrong invocation order")
	}
}

// Catches a hook manager changed and read without a lock; the race detector
// reports it.
func TestKvHooksChangeDuringInvoke(t *testing.T) {
	var root KvHookManager
	// Destroy removes destroyers from the manager's grandparent, so every
	// manager destroyed here needs a grandparent.
	middle := root.Child().Child()
	source := mock.KvClient(&fakeKvClient{})

	var changers sync.WaitGroup
	for i := 0; i < 2; i++ {
		changers.Add(1)
		go func() {
			defer changers.Done()
			for j := 0; j < 200; j++ {
				child := middle.Child().(*KvHookManager)
				child.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
					next()
				})
				child.PushDestroyer(func() {})
				child.Destroy()
				// The middle manager is shared by both goroutines.
				if j%20 == 0 {
					middle.Destroy()
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		changers.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			root.Invoke(source, &memd.Packet{})
		}
	}
}

// Add and Destroy change a manager and its ancestors in one step, so a hook
// added while its manager is destroyed is either removed with it or stays in
// the manager, and never stays in an ancestor alone.
func TestKvHooksAddDuringDestroyLeavesNoAncestorHook(t *testing.T) {
	var root KvHookManager
	middle := root.Child()
	source := mock.KvClient(&fakeKvClient{})
	// The hook ends the chain, so Invoke reaches its end only without it.
	hook := func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {}

	for i := 0; i < 20000; i++ {
		child := middle.Child()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			child.Add(hook)
		}()
		go func() {
			defer wg.Done()
			child.Destroy()
		}()
		wg.Wait()
		child.Destroy()

		if reached := root.Invoke(source, &memd.Packet{}); !reached {
			t.Fatalf("iteration %d: a hook stayed in the root after its manager was destroyed", i)
		}
	}
}
