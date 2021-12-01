package checks

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"sync/atomic"
	"time"
)

type KVHook struct {
	times   uint32
	handler mock.KvHookFunc
	expect  *KvExpect
}

func NewKvHook(expect *KvExpect, handler mock.KvHookFunc) KVHook {
	return KVHook{
		handler: handler,
		expect:  expect,
	}
}

func (hook KVHook) Times(times uint32) *KVHook {
	hook.times = times
	return &hook
}

func (hook KVHook) Cmd(command memd.CmdCode) *KVHook {
	hook.expect = hook.expect.Cmd(command)
	return &hook
}

func (hook KVHook) Key(key string) *KVHook {
	hook.expect = hook.expect.Key(key)
	return &hook
}

func (hook KVHook) Build() mock.KvHookFunc {
	var i uint32
	times := hook.times
	if times == 0 {
		times = 1
	}
	return func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if !hook.expect.match(source, pak) {
			next()
			return
		}
		if atomic.AddUint32(&i, 1) > times {
			next()
			return
		}

		hook.handler(source, pak, start, next)
	}
}
