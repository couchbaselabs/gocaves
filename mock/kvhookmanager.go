package mock

import (
	"context"

	"github.com/couchbase/gocbcore/v9/memd"
)

// KvHookFunc implements a hook for handling a kv packet.
// NOTE: It is safe and expected that a hook may alter the packet.
type KvHookFunc func(source *KvClient, pak *memd.Packet, next func())

// KvHookManager implements a tree of hooks which can handle a kv packet.
type KvHookManager struct {
	hookManager
	ctx       context.Context
	ctxCancel context.CancelFunc
}

// Child returns a child hook manager to this hook manager.
func (m *KvHookManager) Child() *KvHookManager {
	ctx, ctxCancel := context.WithCancel(m.ctx)

	return &KvHookManager{
		hookManager: m.hookManager.Child(),
		ctx:         ctx,
		ctxCancel:   ctxCancel,
	}
}

// Push adds a new hook at the end of the processing chain.
func (m *KvHookManager) Push(fn KvHookFunc) {
	m.hookManager.Push(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *KvHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *KvHookManager) pushDestroyer(fn func()) {
	m.hookManager.PushDestroyer(fn)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.  It returns whether
// the end of the hook chain was reached or not.
func (m *KvHookManager) Invoke(source *KvClient, pak *memd.Packet) bool {
	var successMarker struct{}
	var reachedEndOfChain bool

	m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*KvHookFunc))
		hookFn(source, pak, func() {
			res := next()
			if res == nil {
				// This indicates we reached the end of the chain.
				reachedEndOfChain = true
			}
		})
		return successMarker
	})

	return reachedEndOfChain
}

// Expect sets up a new expectation to wait for.
func (m *KvHookManager) Expect() *KvHookExpect {
	return &KvHookExpect{
		parent: m,
	}
}
