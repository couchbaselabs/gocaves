package mock

// ViewHookFunc implements a hook for handling a query request.
// NOTE: It is safe and expected that a hook may alter the packet.
type ViewHookFunc func(source ViewService, req *HTTPRequest, next func() *HTTPResponse) *HTTPResponse

// ViewHookManager implements a tree of hooks which can handle a view request.
type ViewHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() ViewHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn ViewHookFunc)

	// Destroy removes all this managers hooks from the root manager.
	Destroy()
}
