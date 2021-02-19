package hooks

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// MgmtHookManager implements a tree of hooks which can handle a mgmt request.
type MgmtHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *MgmtHookManager) Child() mock.MgmtHookManager {
	return &MgmtHookManager{
		m.hookManager.Child(),
	}
}

// Add adds a new hook at the end of the processing chain.
func (m *MgmtHookManager) Add(fn mock.MgmtHookFunc) {
	m.hookManager.Add(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *MgmtHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *MgmtHookManager) translateHookResult(val interface{}) *mock.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*mock.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *MgmtHookManager) Invoke(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*mock.MgmtHookFunc))
		return hookFn(source, req, func() *mock.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*mock.HTTPResponse)
}
