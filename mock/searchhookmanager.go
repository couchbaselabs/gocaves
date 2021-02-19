package mock

// SearchHookFunc implements a hook for handling a query request.
// NOTE: It is safe and expected that a hook may alter the packet.
type SearchHookFunc func(source SearchService, req *HTTPRequest, next func() *HTTPResponse) *HTTPResponse

// SearchHookManager implements a tree of hooks which can handle a search request.
type SearchHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() SearchHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn SearchHookFunc)
}
