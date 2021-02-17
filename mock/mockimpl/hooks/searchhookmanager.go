package hooks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// SearchHookManager implements a tree of hooks which can handle a search request.
type SearchHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *SearchHookManager) Child() mock.SearchHookManager {
	return &SearchHookManager{
		m.hookManager.Child(),
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *SearchHookManager) Add(fn mock.SearchHookFunc) {
	m.hookManager.Add(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *SearchHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *SearchHookManager) pushDestroyer(fn func()) {
	m.hookManager.PushDestroyer(fn)
}

func (m *SearchHookManager) translateHookResult(val interface{}) *mock.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*mock.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *SearchHookManager) Invoke(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*mock.SearchHookFunc))
		return hookFn(source, req, func() *mock.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*mock.HTTPResponse)
}
