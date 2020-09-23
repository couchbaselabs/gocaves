package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
)

// RegisterOptions specifies options used for impl registration
type RegisterOptions struct {
	KvInHooks  *hooks.KvHookManager
	KvOutHooks *hooks.KvHookManager
	MgmtHooks  *hooks.MgmtHookManager
}

// Register registers all known hooks.
func Register(opts RegisterOptions) {
	(&kvImplAuth{}).Register(opts.KvInHooks)
	(&kvImplCccp{}).Register(opts.KvInHooks)
	(&kvImplCrud{}).Register(opts.KvInHooks)
	(&kvImplErrMap{}).Register(opts.KvInHooks)
	(&kvImplHello{}).Register(opts.KvInHooks)
	(&mgmtImplConfig{}).Register(opts.MgmtHooks)
}

func replyWithError(source mock.KvClient, pak *memd.Packet, err error) {
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusInternalError,
		Value:   []byte(err.Error()),
	})
}
