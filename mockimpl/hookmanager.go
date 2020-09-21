package mockimpl

// KvHookManager implements a tree of hooks which can handle a KV packet.
type hookManager struct {
	parent       *hookManager
	destroyHooks []*func()
	hooks        []interface{}
}

// Child returns a child hook manager to this hook manager.
func (m *hookManager) Child() hookManager {
	return hookManager{
		parent: m,
	}
}

// Push adds a new hook at the end of the processing chain.
func (m *hookManager) Push(fn interface{}) {
	m.hooks = append(m.hooks, fn)
	if m.parent != nil {
		m.parent.Push(fn)
	}
}

func (m *hookManager) pushDestroyer(fnPtr *func()) {
	m.destroyHooks = append(m.destroyHooks, fnPtr)
	if m.parent != nil {
		m.parent.pushDestroyer(fnPtr)
	}
}

func (m *hookManager) PushDestroyer(fn func()) {
	fnPtr := &fn
	m.pushDestroyer(fnPtr)
}

// remove will remote a specific hook pointer from this manager.
func (m *hookManager) remove(fn interface{}) {
	newHooks := make([]interface{}, len(m.hooks))
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
	newDestroyHooks := make([]*func(), len(m.destroyHooks))
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
	if m.parent != nil {
		for _, hook := range m.hooks {
			m.parent.remove(hook)
		}

		for _, hook := range m.destroyHooks {
			m.parent.parent.removeDestroyer(hook)
		}
	}

	for _, fn := range m.destroyHooks {
		(*fn)()
	}

	m.hooks = nil
	m.destroyHooks = nil
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *hookManager) Invoke(fn func(interface{}, func() interface{}) interface{}) interface{} {
	hookChain := make([]interface{}, len(m.hooks))
	copy(hookChain, m.hooks)

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
