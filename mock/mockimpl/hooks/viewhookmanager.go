package hooks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// ViewHookManager implements a tree of hooks which can handle a view request.
type ViewHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *ViewHookManager) Child() mock.ViewHookManager {
	return &ViewHookManager{
		m.hookManager.Child(),
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *ViewHookManager) Add(fn mock.ViewHookFunc) {
	m.hookManager.Add(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *ViewHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *ViewHookManager) translateHookResult(val interface{}) *mock.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*mock.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *ViewHookManager) Invoke(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*mock.ViewHookFunc))
		return hookFn(source, req, func() *mock.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*mock.HTTPResponse)
}
