package svcimpls

import (
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"time"
)

// RegisterOptions specifies options used for impl registration
type RegisterOptions struct {
	AnalyticsHooks mock.AnalyticsHookManager
	KvInHooks      mock.KvHookManager
	KvOutHooks     mock.KvHookManager
	MgmtHooks      mock.MgmtHookManager
	QueryHooks     mock.QueryHookManager
	SearchHooks    mock.SearchHookManager
	ViewHooks      mock.ViewHookManager
}

// Register registers all known hooks. It returns whether kv_engine's request
// validator accepts a KV request, which it checks as it reads the request.
func Register(opts RegisterOptions) func(source mock.KvClient, pak *memd.Packet) bool {
	h := &hookHelper{
		AnalyticsHooks: opts.AnalyticsHooks,
		KvInHooks:      opts.KvInHooks,
		KvOutHooks:     opts.KvOutHooks,
		MgmtHooks:      opts.MgmtHooks,
		QueryHooks:     opts.QueryHooks,
		SearchHooks:    opts.SearchHooks,
		ViewHooks:      opts.ViewHooks,
	}

	(&analyticsImplPing{}).Register(h)
	(&kvImplAuth{}).Register(h)
	(&kvImplCccp{}).Register(h)
	(&kvImplCrud{}).Register(h)
	(&kvImplErrMap{}).Register(h)
	(&kvImplHello{}).Register(h)
	(&kvImplPing{}).Register(h)
	(&queryImplPing{}).Register(h)
	(&searchImplPing{}).Register(h)
	(&viewImplPing{}).Register(h)
	(&viewImplMgmt{}).Register(h)
	(&viewImplQuery{}).Register(h)
	(&mgmtImpl{}).Register(h)

	// Hooks run in reverse order of registration, and frame validation must
	// run before any command handler.
	(&kvImplFrames{}).Register(h)

	return func(source mock.KvClient, pak *memd.Packet) bool {
		status, _ := h.validateRequest(source, pak)
		return status == memd.StatusSuccess
	}
}

func replyWithError(source mock.KvClient, pak *memd.Packet, start time.Time, err error) {
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusInternalError,
		Value:   []byte(err.Error()),
	}, start)
}
