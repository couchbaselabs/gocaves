package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/helpers/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
)

// hookHelper is simply a wrapper to simplify the setup of hooks.
type hookHelper struct {
	KvInHooks  mock.KvHookManager
	KvOutHooks mock.KvHookManager
	MgmtHooks  mock.MgmtHookManager
}

// RegisterKvReq registers a hook for a kv command request.
func (h *hookHelper) RegisterKvHandler(cmd memd.CmdCode, handler func(source mock.KvClient, pak *memd.Packet)) {
	h.KvInHooks.Add(func(source mock.KvClient, pak *memd.Packet, next func()) {
		if pak.Magic == memd.CmdMagicReq && pak.Command == cmd {
			handler(source, pak)
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
