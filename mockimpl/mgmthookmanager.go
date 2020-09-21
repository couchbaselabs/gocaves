package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// MgmtHookFunc implements a hook for handling a mgmt request.
// NOTE: It is safe and expected that a hook may alter the packet.
type MgmtHookFunc func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse

// MgmtHookManager implements a tree of hooks which can handle a mgmt request.
type MgmtHookManager struct {
	hookManager
}

// Child returns a child hook manager to this hook manager.
func (m *MgmtHookManager) Child() *MgmtHookManager {
	return &MgmtHookManager{
		m.hookManager.Child(),
	}
}

// Push adds a new hook at the end of the processing chain.
func (m *MgmtHookManager) Push(fn MgmtHookFunc) {
	m.hookManager.Push(&fn)
}

// Destroy removes all hooks that were added to this manager.
func (m *MgmtHookManager) Destroy() {
	m.hookManager.Destroy()
}

func (m *MgmtHookManager) pushDestroyer(fn func()) {
	m.hookManager.PushDestroyer(fn)
}

func (m *MgmtHookManager) translateHookResult(val interface{}) *servers.HTTPResponse {
	if val == nil {
		return nil
	}
	return val.(*servers.HTTPResponse)
}

// Invoke will invoke this hook chain.  It starts at the most recently
// registered hook and works it's way to the oldest hook.
func (m *MgmtHookManager) Invoke(source *MgmtService, req *servers.HTTPRequest) *servers.HTTPResponse {
	res := m.hookManager.Invoke(func(hook interface{}, next func() interface{}) interface{} {
		hookFn := *(hook.(*MgmtHookFunc))
		return hookFn(source, req, func() *servers.HTTPResponse {
			return m.translateHookResult(next())
		})
	})
	return res.(*servers.HTTPResponse)
}

// Expect sets up a new expectation to wait for.
func (m *MgmtHookManager) Expect() *MgmtHookExpect {
	return &MgmtHookExpect{
		parent: m,
	}
}
