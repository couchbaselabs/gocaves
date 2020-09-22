package svcimpls

import "github.com/couchbaselabs/gocaves/hooks"

// RegisterOptions specifies options used for impl registration
type RegisterOptions struct {
	KvInHooks  *hooks.KvHookManager
	KvOutHooks *hooks.KvHookManager
	MgmtHooks  *hooks.MgmtHookManager
}

// Register registers all known hooks.
func Register(opts RegisterOptions) {
	(&kvImplHello{}).Register(opts.KvInHooks)
	(&kvImplAuth{}).Register(opts.KvInHooks)
	(&kvImplCccp{}).Register(opts.KvInHooks)
	(&kvImplCrud{}).Register(opts.KvInHooks)
	(&mgmtImplConfig{}).Register(opts.MgmtHooks)
}
