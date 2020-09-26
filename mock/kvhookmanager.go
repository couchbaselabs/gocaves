package mock

import "github.com/couchbase/gocbcore/v9/memd"

// KvHookFunc implements a hook for handling a kv packet.
// NOTE: It is safe and expected that a hook may alter the packet.
type KvHookFunc func(source KvClient, pak *memd.Packet, next func())

// KvHookManager implements a tree of hooks which can handle a kv packet.
type KvHookManager interface {
	// Child returns a child hook manager to this hook manager.
	Child() KvHookManager

	// Add adds a new hook at the end of the processing chain.
	Add(fn KvHookFunc)
}