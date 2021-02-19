package hooks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// QueryHookManager implements a tree of hooks which can handle a query request.
type QueryHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *QueryHookManager) Child() mock.QueryHookManager {
	return &QueryHookManager{
		m.hookManager.Child(),
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *QueryHookManager) Add(fn mock.QueryHookFunc) {
	m.hookManager.Add(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *QueryHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *QueryHookManager) translateHookResult(val interface{}) *mock.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*mock.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *QueryHookManager) Invoke(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*mock.QueryHookFunc))
		return hookFn(source, req, func() *mock.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*mock.HTTPResponse)
}
