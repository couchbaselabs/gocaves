package hooks

import (
	"testing"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/scramserver"
	tmock "github.com/stretchr/testify/mock"
)

type fakeKvClient struct {
	tmock.Mock
}

func (c *fakeKvClient) Source() mock.KvService                   { return nil }
func (c *fakeKvClient) ScramServer() *scramserver.ScramServer    { return nil }
func (c *fakeKvClient) SetAuthenticatedUserName(userName string) {}
func (c *fakeKvClient) AuthenticatedUserName() string            { return "" }
func (c *fakeKvClient) SetSelectedBucketName(bucketName string)  {}
func (c *fakeKvClient) SelectedBucketName() string               { return "" }
func (c *fakeKvClient) SelectedBucket() mock.Bucket              { return nil }
func (c *fakeKvClient) WritePacket(pak *memd.Packet) error       { return nil }
func (c *fakeKvClient) Close() error                             { return nil }

func TestKvHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := mock.KvClient(&fakeKvClient{})
	fakePacket := &memd.Packet{}

	var hooks KvHookManager
	hooks.Push(func(source mock.KvClient, pak *memd.Packet, next func()) {
		hookInvokes = append(hookInvokes, 1)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Push(func(source mock.KvClient, pak *memd.Packet, next func()) {
		hookInvokes = append(hookInvokes, 2)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Push(func(source mock.KvClient, pak *memd.Packet, next func()) {
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
