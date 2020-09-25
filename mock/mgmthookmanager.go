package mock

// MgmtHookFunc implements a hook for handling a mgmt request.
// NOTE: It is safe and expected that a hook may alter the packet.
type MgmtHookFunc func(source MgmtService, req *HTTPRequest, next func() *HTTPResponse) *HTTPResponse

// MgmtHookManager implements a tree of hooks which can handle a mgmt request.
type MgmtHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() MgmtHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn MgmtHookFunc)
}
