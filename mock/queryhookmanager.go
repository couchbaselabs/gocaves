package mock

// QueryHookFunc implements a hook for handling a query request.
// NOTE: It is safe and expected that a hook may alter the packet.
type QueryHookFunc func(source QueryService, req *HTTPRequest, next func() *HTTPResponse) *HTTPResponse

// QueryHookManager implements a tree of hooks which can handle a query request.
type QueryHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() QueryHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn QueryHookFunc)

	// Destroy removes all this managers hooks from the root manager.
	Destroy()
}
