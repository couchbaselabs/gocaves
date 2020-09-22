package mockimpl

import (
	"testing"

	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

func TestMgmtHooksBasic(t *testing.T) {
	hookInvokes := make([]int, 0)

	fakeSource := &MgmtService{}
	fakeRequest := &servers.HTTPRequest{}
	fakeResponse1 := &servers.HTTPResponse{}
	fakeResponse2 := &servers.HTTPResponse{}
	fakeResponse3 := &servers.HTTPResponse{}

	var hooks MgmtHookManager
	hooks.Push(func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
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
	hooks.Push(func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
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
	hooks.Push(func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
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