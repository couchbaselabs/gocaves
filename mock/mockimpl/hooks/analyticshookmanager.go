package hooks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// AnalyticsHookManager implements a tree of hooks which can handle an analytics request.
type AnalyticsHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *AnalyticsHookManager) Child() mock.AnalyticsHookManager {
	return &AnalyticsHookManager{
		m.hookManager.Child(),
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *AnalyticsHookManager) Add(fn mock.AnalyticsHookFunc) {
	m.hookManager.Add(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *AnalyticsHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *AnalyticsHookManager) pushDestroyer(fn func()) {
	m.hookManager.PushDestroyer(fn)
}

func (m *AnalyticsHookManager) translateHookResult(val interface{}) *mock.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*mock.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *AnalyticsHookManager) Invoke(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*mock.AnalyticsHookFunc))
		return hookFn(source, req, func() *mock.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*mock.HTTPResponse)
}
