package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"time"
)

// hookHelper is simply a wrapper to simplify the setup of hooks.
type hookHelper struct {
	AnalyticsHooks mock.AnalyticsHookManager
	KvInHooks      mock.KvHookManager
	KvOutHooks     mock.KvHookManager
	MgmtHooks      mock.MgmtHookManager
	QueryHooks     mock.QueryHookManager
	SearchHooks    mock.SearchHookManager
	ViewHooks      mock.ViewHookManager
}

// RegisterKvReq registers a hook for a kv command request.
func (h *hookHelper) RegisterKvHandler(cmd memd.CmdCode, handler func(source mock.KvClient, pak *memd.Packet, start time.Time)) {
	h.KvInHooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Magic == memd.CmdMagicReq && pak.Command == cmd {
			handler(source, pak, start)
		} else {
			next()
		}
	})
}

// RegisterMgmtReq registers a hook for a mgmt request.
func (h *hookHelper) RegisterMgmtHandler(method, path string, handler func(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse) {
	parser := pathparse.NewParser(path)
	h.MgmtHooks.Add(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if req.Method == method && parser.Match(req.URL.Path) {
			return handler(source, req)
		}
		return next()
	})
}

// RegisterQueryHandler registers a hook for a query request.
func (h *hookHelper) RegisterQueryHandler(method, path string, handler func(source mock.QueryService, req *mock.HTTPRequest) *mock.HTTPResponse) {
	parser := pathparse.NewParser(path)
	h.QueryHooks.Add(func(source mock.QueryService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if req.Method == method && parser.Match(req.URL.Path) {
			return handler(source, req)
		}
		return next()
	})
}

// RegisterAnalyticsHandler registers a hook for an analytics request.
func (h *hookHelper) RegisterAnalyticsHandler(method, path string, handler func(source mock.AnalyticsService, req *mock.HTTPRequest) *mock.HTTPResponse) {
	parser := pathparse.NewParser(path)
	h.AnalyticsHooks.Add(func(source mock.AnalyticsService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if req.Method == method && parser.Match(req.URL.Path) {
			return handler(source, req)
		}
		return next()
	})
}

// RegisterSearchHandler registers a hook for a search request.
func (h *hookHelper) RegisterSearchHandler(method, path string, handler func(source mock.SearchService, req *mock.HTTPRequest) *mock.HTTPResponse) {
	parser := pathparse.NewParser(path)
	h.SearchHooks.Add(func(source mock.SearchService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if req.Method == method && parser.Match(req.URL.Path) {
			return handler(source, req)
		}
		return next()
	})
}

// RegisterViewHandler registers a hook for a view request.
func (h *hookHelper) RegisterViewHandler(method, path string, handler func(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse) {
	parser := pathparse.NewParser(path)
	h.ViewHooks.Add(func(source mock.ViewService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		if req.Method == method && parser.Match(req.URL.Path) {
			return handler(source, req)
		}
		return next()
	})
}
