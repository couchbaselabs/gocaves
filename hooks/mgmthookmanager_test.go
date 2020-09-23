package hooks

import (
	"testing"

	tmock "github.com/stretchr/testify/mock"

	"github.com/couchbaselabs/gocaves/mock"
)

type fakeMgmtService struct {
	tmock.Mock
}

func (m *fakeMgmtService) Node() mock.ClusterNode { return nil }
func (m *fakeMgmtService) Hostname() string       { return "" }
func (m *fakeMgmtService) ListenPort() int        { return 0 }
func (m *fakeMgmtService) ListenPortTLS() int     { return 0 }
func (m *fakeMgmtService) Close() error           { return nil }

func TestMgmtHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := mock.MgmtService(&fakeMgmtService{})
	fakeRequest := &mock.HTTPRequest{}
	fakeResponse1 := &mock.HTTPResponse{}
	fakeResponse2 := &mock.HTTPResponse{}
	fakeResponse3 := &mock.HTTPResponse{}

	var hooks MgmtHookManager
	hooks.Push(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		hookInvokes = append(hookInvokes, 1)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if req != fakeRequest {
			t.Fatalf("failed to pass the packet")
		}
		res := next()
		if res != nil {
			t.Fatalf("next call returned wrong value")
		}
		return fakeResponse1
	})
	hooks.Push(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		hookInvokes = append(hookInvokes, 2)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if req != fakeRequest {
			t.Fatalf("failed to pass the packet")
		}
		res := next()
		if res != fakeResponse1 {
			t.Fatalf("next call returned wrong value")
		}
		return fakeResponse2
	})
	hooks.Push(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
		hookInvokes = append(hookInvokes, 3)
		if source != fakeSource {
			t.Fatalf("failed to pass the source")
		}
		if req != fakeRequest {
			t.Fatalf("failed to pass the packet")
		}
		res := next()
		if res != fakeResponse2 {
			t.Fatalf("next call returned wrong value")
		}
		return fakeResponse3
	})

	res := hooks.Invoke(fakeSource, fakeRequest)

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
	if res != fakeResponse3 {
		t.Fatalf("invoke returned wrong value")
	}
}
