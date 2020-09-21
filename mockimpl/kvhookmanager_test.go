package mockimpl

import (
	"testing"

	"github.com/couchbase/gocbcore/v9/memd"
)

func TestKvHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := &KvClient{}
	fakePacket := &memd.Packet{}

	var hooks KvHookManager
	hooks.Push(func(source *KvClient, pak *memd.Packet, next func()) {
		hookInvokes = append(hookInvokes, 1)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Push(func(source *KvClient, pak *memd.Packet, next func()) {
		hookInvokes = append(hookInvokes, 2)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Push(func(source *KvClient, pak *memd.Packet, next func()) {
		hookInvokes = append(hookInvokes, 3)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})

	hooks.Invoke(fakeSource, fakePacket)

	if len(hookInvokes) != 3 {
		t.Fatalf("wrong number of invocations")
	}
	if hookInvokes[0] != 3 {
		t.Fatalf("wrong invocation order")
	}
	if hookInvokes[1] != 2 {
		t.Fatalf("wrong invocation order")
	}
	if hookInvokes[2] != 1 {
		t.Fatalf("wrong invocation order")
	}
}
