package checks

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type memdPakFields int

const (
	memdPakFieldMagic          = 1 << 0
	memdPakFieldCmd            = 1 << 1
	memdPakFieldKey            = 1 << 2
	memdPakFieldOpaque         = 1 << 3
	memdPakFieldCollectionID   = 1 << 4
	memdPakFieldBucketName     = 1 << 5
	memdPakFieldScopeName      = 1 << 6
	memdPakFieldCollectionName = 1 << 7
)

// KvExpect represents a Kv expectation.
type KvExpect struct {
	parent               *T
	expectSource         mock.KvClient
	expectFields         memdPakFields
	expectMagic          memd.CmdMagic
	expectCmd            memd.CmdCode
	expectOpaque         uint32
	expectKey            []byte
	expectCollectionID   uint32
	expectBucketName     string
	expectScopeName      string
	expectCollectionName string
	expectFns            []func(mock.KvClient, *memd.Packet) bool
}

func (e KvExpect) String() string {
	var expectPieces []string
	addExpectation := func(format string, args ...interface{}) {
		expectPieces = append(expectPieces, fmt.Sprintf(format, args...))
	}

	if e.expectFields&memdPakFieldMagic != 0 {
		addExpectation("Magic: %02x", e.expectMagic)
	}
	if e.expectFields&memdPakFieldCmd != 0 {
		addExpectation("Command: %s (%02x)", e.expectCmd.Name(), e.expectCmd)
	}
	if e.expectFields&memdPakFieldKey != 0 {
		addExpectation("Key: %s (%v)", e.expectKey, e.expectKey)
	}
	if e.expectFields&memdPakFieldOpaque != 0 {
		addExpectation("Opaque: %08x", e.expectOpaque)
	}
	if e.expectFields&memdPakFieldCollectionID != 0 {
		addExpectation("CollectionID: %d", e.expectCollectionID)
	}
	if e.expectFields&memdPakFieldBucketName != 0 {
		addExpectation("BucketName: %s", e.expectBucketName)
	}
	if e.expectFields&memdPakFieldScopeName != 0 {
		addExpectation("ScopeName: %s", e.expectScopeName)
	}
	if e.expectFields&memdPakFieldCollectionName != 0 {
		addExpectation("CollectionName: %s", e.expectCollectionName)
	}
	if e.expectSource != nil {
		addExpectation("withSource=YES")
	}
	if len(e.expectFns) > 0 {
		addExpectation("withCustomCheck=%d", len(e.expectFns))
	}

	return fmt.Sprintf("{ %s }", strings.Join(expectPieces, ", "))
}

// ReplyTo specifies to expect the reply to another packet.
func (e KvExpect) ReplyTo(source mock.KvClient, pak *memd.Packet) *KvExpect {
	return e.Source(source).Opaque(pak.Opaque)
}

// Source specifies a specific source which is expected.
func (e KvExpect) Source(cli mock.KvClient) *KvExpect {
	e.expectSource = cli
	return &e
}

// Magic specifies a specific magic which is expected.
func (e KvExpect) Magic(magic memd.CmdMagic) *KvExpect {
	e.expectFields |= memdPakFieldMagic
	e.expectMagic = magic
	return &e
}

// Cmd specifies a specific cmd which is expected.
func (e KvExpect) Cmd(cmd memd.CmdCode) *KvExpect {
	e.expectFields |= memdPakFieldCmd
	e.expectCmd = cmd
	return &e
}

// KeyBytes specifies a specific key which is expected as bytes.
func (e KvExpect) KeyBytes(key []byte) *KvExpect {
	e.expectFields |= memdPakFieldKey
	e.expectKey = key
	return &e
}

// Key specifies a specific key which is expected.
func (e KvExpect) Key(key string) *KvExpect {
	return e.KeyBytes([]byte(key))
}

// Opaque specifies a specific opaque which is expected.
func (e KvExpect) Opaque(opaque uint32) *KvExpect {
	e.expectFields |= memdPakFieldOpaque
	e.expectOpaque = opaque
	return &e
}

// BucketName specifies a specific bucket name which is expected.
func (e KvExpect) BucketName(name string) *KvExpect {
	e.expectFields |= memdPakFieldBucketName
	e.expectBucketName = name
	return &e
}

// CollectionID specifies a specific collection id which is expected.
func (e KvExpect) CollectionID(id uint32) *KvExpect {
	e.expectFields |= memdPakFieldCollectionID
	e.expectCollectionID = id
	return &e
}

// ScopeName specifies a specific scope name which is expected.
func (e KvExpect) ScopeName(name string) *KvExpect {
	e.expectFields |= memdPakFieldScopeName
	e.expectScopeName = name
	return &e
}

// CollectionName specifies a specific collection name which is expected.
func (e KvExpect) CollectionName(name string) *KvExpect {
	e.expectFields |= memdPakFieldCollectionName
	e.expectCollectionName = name
	return &e
}

// Custom allows specifying custom logic to use to check the packet.
func (e KvExpect) Custom(chkFn func(mock.KvClient, *memd.Packet) bool) *KvExpect {
	e.expectFns = append(e.expectFns, chkFn)
	return &e
}

// Match checks if this KvExpect matches a particular source and packet.
func (e KvExpect) match(source mock.KvClient, pak *memd.Packet, start time.Time) bool {
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
	if e.expectFields&memdPakFieldKey != 0 && !bytes.Equal(pak.Key, e.expectKey) {
		shouldReject = true
	}
	if e.expectFields&memdPakFieldOpaque != 0 && pak.Opaque != e.expectOpaque {
		shouldReject = true
	}
	if e.expectFields&memdPakFieldCollectionID != 0 && pak.CollectionID != e.expectCollectionID {
		shouldReject = true
	}

	bucket := source.SelectedBucket()
	if e.expectFields&(memdPakFieldBucketName) != 0 {
		if bucket != nil {
			if bucket.Name() != e.expectBucketName {
				shouldReject = true
			}
		} else {
			if e.expectBucketName != "" {
				shouldReject = true
			}
		}
	}

	if e.expectFields&(memdPakFieldScopeName|memdPakFieldCollectionName) != 0 {
		if bucket != nil {
			scopeName, collName := bucket.CollectionManifest().GetByID(pak.CollectionID)
			if e.expectFields&memdPakFieldScopeName != 0 && scopeName != e.expectScopeName {
				shouldReject = true
			}
			if e.expectFields&memdPakFieldCollectionName != 0 && collName != e.expectCollectionName {
				shouldReject = true
			}
		} else {
			// If we were expecting a scope/collection, but there is no bucket, that's not possible.
			shouldReject = true
		}
	}

	for _, chkFn := range e.expectFns {
		if !chkFn(source, pak) {
			shouldReject = true
			break
		}
	}

	return !shouldReject
}

// Wait will wait until this expectation is satisfied.
func (e KvExpect) Wait() (mock.KvClient, *memd.Packet) {
	var sourceOut mock.KvClient
	var pakOut *memd.Packet

	waitCh := make(chan struct{})
	hasTripped := uint32(0)

	handler := func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if !e.match(source, pak, start) {
			next()
			return
		}

		next()

		if atomic.CompareAndSwapUint32(&hasTripped, 0, 1) {
			sourceOut = source
			pakOut = pak
			waitCh <- struct{}{}
		}
	}
	e.parent.testKvInHooks().Add(handler)
	e.parent.testKvOutHooks().Add(handler)

	select {
	case <-waitCh:
	case <-e.parent.cancelCh:
		e.parent.Fatalf("Test ended while still waiting for kv packet %s", e)
	}

	return sourceOut, pakOut
}
