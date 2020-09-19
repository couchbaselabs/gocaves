package mock

import (
	"bytes"
	"errors"
	"sync"

	"github.com/couchbase/gocbcore/v9/memd"
)

type memdPakFields int

const (
	memdPakFieldMagic  = 1 << 0
	memdPakFieldCmd    = 1 << 1
	memdPakFieldKey    = 1 << 2
	memdPakFieldOpaque = 1 << 3
)

// KvHookExpect provides a nicer way to configure kv hooks.
type KvHookExpect struct {
	parent       *KvHookManager
	expectSource *KvClient
	expectFields memdPakFields
	expectMagic  memd.CmdMagic
	expectCmd    memd.CmdCode
	expectOpaque uint32
	expectKey    []byte
}

// ReplyTo specifies to expect the reply to another packet.
func (e KvHookExpect) ReplyTo(source *KvClient, pak *memd.Packet) *KvHookExpect {
	return e.Source(source).Opaque(pak.Opaque)
}

// Source specifies a specific source which is expected.
func (e KvHookExpect) Source(cli *KvClient) *KvHookExpect {
	e.expectSource = cli
	return &e
}

// Magic specifies a specific magic which is expected.
func (e KvHookExpect) Magic(magic memd.CmdMagic) *KvHookExpect {
	e.expectFields |= memdPakFieldMagic
	e.expectMagic = magic
	return &e
}

// Cmd specifies a specific cmd which is expected.
func (e KvHookExpect) Cmd(cmd memd.CmdCode) *KvHookExpect {
	e.expectFields |= memdPakFieldCmd
	e.expectCmd = cmd
	return &e
}

// KeyBytes specifies a specific key which is expected as bytes.
func (e KvHookExpect) KeyBytes(key []byte) *KvHookExpect {
	e.expectFields |= memdPakFieldKey
	e.expectKey = key
	return &e
}

// Key specifies a specific key which is expected.
func (e KvHookExpect) Key(key string) *KvHookExpect {
	return e.KeyBytes([]byte(key))
}

// Opaque specifies a specific opaque which is expected.
func (e KvHookExpect) Opaque(opaque uint32) *KvHookExpect {
	e.expectFields |= memdPakFieldOpaque
	e.expectOpaque = opaque
	return &e
}

// Handler specifies the handler to invoke if the expectations are met.
func (e KvHookExpect) Handler(fn func(source *KvClient, pak *memd.Packet, next func())) *KvHookExpect {
	e.parent.Push(func(source *KvClient, pak *memd.Packet, next func()) {
		shouldReject := false
		if e.expectSource != nil && source != e.expectSource {
			shouldReject = true
		}
		if e.expectFields&memdPakFieldMagic != 0 && pak.Magic != e.expectMagic {
			shouldReject = true
		}
		if e.expectFields&memdPakFieldCmd != 0 && pak.Command != e.expectCmd {
			shouldReject = true
		}
		if e.expectFields&memdPakFieldKey != 0 && bytes.Compare(pak.Key, e.expectKey) != 0 {
			shouldReject = true
		}
		if e.expectFields&memdPakFieldOpaque != 0 && pak.Opaque != e.expectOpaque {
			shouldReject = true
		}

		if shouldReject {
			next()
			return
		}

		fn(source, pak, next)
	})

	return &e
}

// Wait waits until the specific expectation is triggered.
func (e KvHookExpect) Wait(checkFn func(*KvClient, *memd.Packet) bool) (*KvClient, *memd.Packet) {
	var sourceOut *KvClient
	var pakOut *memd.Packet
	var panicErr error

	var waitGrp sync.WaitGroup
	waitGrp.Add(1)

	e.parent.pushDestroyer(func() {
		panicErr = errors.New("wait ended due to destroyed hook manager")
		waitGrp.Done()
	})
	e.Handler(func(source *KvClient, pak *memd.Packet, next func()) {
		if checkFn(source, pak) {
			sourceOut = source
			pakOut = pak
			waitGrp.Done()
		}
	})

	waitGrp.Wait()

	if panicErr != nil {
		panic(panicErr)
	}

	return sourceOut, pakOut
}
