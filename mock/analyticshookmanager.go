package mock

// AnalyticsHookFunc implements a hook for handling a query request.
// NOTE: It is safe and expected that a hook may alter the packet.
type AnalyticsHookFunc func(source AnalyticsService, req *HTTPRequest, next func() *HTTPResponse) *HTTPResponse

// AnalyticsHookManager implements a tree of hooks which can handle an analytics request.
type AnalyticsHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() AnalyticsHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn AnalyticsHookFunc)
}
