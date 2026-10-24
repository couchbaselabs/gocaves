package hooks

import (
	"sync"
	"sync/atomic"
)

// KvHookManager implements a tree of hooks which can handle a KV packet.
type hookManager struct {
	parent *hookManager

	// lock, in the root manager, guards destroyHooks and hooks of every
	// manager in the tree. Tests change the hooks while connections invoke
	// them, and Add and Destroy change a manager and its ancestors in one
	// step under it. It is never held while a hook or destroyer runs.
	lock         sync.Mutex
	destroyHooks []*func()
	hooks        []interface{}
}

// treeLock returns the lock of the root of the manager's tree.
func (m *hookManager) treeLock() *sync.Mutex {
	for m.parent != nil {
		m = m.parent
	}
	return &m.lock
}

// Child returns a child hook manager to this hook manager.
func (m *hookManager) Child() hookManager {
	return hookManager{
		parent: m,
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *hookManager) Add(fn interface{}) {
	lock := m.treeLock()
	lock.Lock()
	defer lock.Unlock()
	m.add(fn)
}

func (m *hookManager) add(fn interface{}) {
	m.hooks = append(m.hooks, fn)
	if m.parent != nil {
		m.parent.add(fn)
	}
}

func (m *hookManager) pushDestroyer(fnPtr *func()) {
	m.destroyHooks = append(m.destroyHooks, fnPtr)
	if m.parent != nil {
		m.parent.pushDestroyer(fnPtr)
	}
}

// PushDestroyer registers fn to run when this manager or any ancestor is
// destroyed. Each of those managers holds it, and it runs once, whichever is
// destroyed first.
func (m *hookManager) PushDestroyer(fn func()) {
	var ran atomic.Bool
	once := func() {
		if ran.CompareAndSwap(false, true) {
			fn()
		}
	}
	fnPtr := &once
	lock := m.treeLock()
	lock.Lock()
	defer lock.Unlock()
	m.pushDestroyer(fnPtr)
}

// remove will remote a specific hook pointer from this manager.
func (m *hookManager) remove(fn interface{}) {
	newHooks := make([]interface{}, 0)
	for _, hook := range m.hooks {
		if hook != fn {
			newHooks = append(newHooks, hook)
		}
	}

	m.hooks = newHooks

	if m.parent != nil {
		m.parent.remove(fn)
	}
}

// removeDestroyer will remote a specified destroyer from this manager.
func (m *hookManager) removeDestroyer(fn *func()) {
	newDestroyHooks := make([]*func(), 0)
	for _, hook := range m.destroyHooks {
		if hook != fn {
			newDestroyHooks = append(newDestroyHooks, hook)
		}
	}

	m.destroyHooks = newDestroyHooks

	if m.parent != nil {
		m.parent.removeDestroyer(fn)
	}
}

// Destroy removes all hooks that were added to this manager.
func (m *hookManager) Destroy() {
	lock := m.treeLock()
	lock.Lock()
	destroyHooks := m.destroyHooks
	if m.parent != nil {
		for _, hook := range m.hooks {
			m.parent.remove(hook)
		}

		for _, hook := range m.destroyHooks {
			m.parent.removeDestroyer(hook)
		}
	}
	m.hooks = nil
	m.destroyHooks = nil
	lock.Unlock()

	for _, fn := range destroyHooks {
		(*fn)()
	}
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *hookManager) Invoke(fn func(interface{}, func() interface{}) interface{}) interface{} {
	lock := m.treeLock()
	lock.Lock()
	hookChain := make([]interface{}, len(m.hooks))
	copy(hookChain, m.hooks)
	lock.Unlock()

	curHookIdx := len(hookChain)
	var nextHook func() interface{}
	nextHook = func() interface{} {
		if curHookIdx == 0 {
			return nil
		}

		curHookIdx--
		return fn(hookChain[curHookIdx], nextHook)
	}

	return nextHook()
}
