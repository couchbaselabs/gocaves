package hooks

import (
	"net"
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mockauth"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock"
	tmock "github.com/stretchr/testify/mock"
)

type fakeKvClient struct {
	tmock.Mock
}

func (c *fakeKvClient) LocalAddr() net.Addr                       { return &net.IPAddr{} }
func (c *fakeKvClient) RemoteAddr() net.Addr                      { return &net.IPAddr{} }
func (c *fakeKvClient) IsTLS() bool                               { return false }
func (c *fakeKvClient) Source() mock.KvService                    { return nil }
func (c *fakeKvClient) ScramServer() *scramserver.ScramServer     { return nil }
func (c *fakeKvClient) SetAuthenticatedUserName(userName string)  {}
func (c *fakeKvClient) AuthenticatedUserName() string             { return "" }
func (c *fakeKvClient) SetSelectedBucketName(bucketName string)   {}
func (c *fakeKvClient) SelectedBucketName() string                { return "" }
func (c *fakeKvClient) SelectedBucket() mock.Bucket               { return nil }
func (c *fakeKvClient) SetFeatures(features []memd.HelloFeature)  {}
func (c *fakeKvClient) HasFeature(feature memd.HelloFeature) bool { return false }
func (c *fakeKvClient) WritePacket(pak *memd.Packet) error        { return nil }
func (c *fakeKvClient) Close() error                              { return nil }
func (c *fakeKvClient) CheckAuthenticated(permission mockauth.Permission, collectionID uint32) bool {
	return true
}

func TestKvHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := mock.KvClient(&fakeKvClient{})
	fakePacket := &memd.Packet{}

	var hooks KvHookManager
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		hookInvokes = append(hookInvokes, 1)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		hookInvokes = append(hookInvokes, 2)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if pak != fakePacket {
			t.Fatalf("failed to pass the packet")
		}
		next()
	})
	hooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
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
