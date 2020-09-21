package mockimpl

import (
	"errors"
	"sync"

	"github.com/couchbaselabs/gocaves/mockimpl/servers"
)

// MgmtHookExpect provides a nicer way to configure mgmt hooks.
type MgmtHookExpect struct {
	parent       *MgmtHookManager
	expectMethod string
	expectPath   *PathParser
}

// Method specifies a specific method which is expected.
func (e MgmtHookExpect) Method(method string) *MgmtHookExpect {
	e.expectMethod = method
	return &e
}

// Path specifies a specific path which is expected.
func (e MgmtHookExpect) Path(path string) *MgmtHookExpect {
	e.expectPath = NewPathParser(path)
	return &e
}

// Handler specifies the handler to invoke if the expectations are met.
func (e MgmtHookExpect) Handler(fn func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse) *MgmtHookExpect {
	e.parent.Push(func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
		if e.expectMethod != "" && req.Method != e.expectMethod {
			return next()
		}

		if e.expectPath != nil && !e.expectPath.Match(req.URL.Path) {
			return next()
		}

		return fn(source, req, next)
	})

	return &e
}

// Wait waits until the specific expectation is triggered.
func (e MgmtHookExpect) Wait(checkFn func(*MgmtService, *servers.HTTPRequest) bool) (*MgmtService, *servers.HTTPRequest) {
	var sourceOut *MgmtService
	var reqOut *servers.HTTPRequest
	var panicErr error

	var waitGrp sync.WaitGroup
	waitGrp.Add(1)

	e.parent.pushDestroyer(func() {
		panicErr = errors.New("wait ended due to destroyed hook manager")
		waitGrp.Done()
	})
	e.Handler(func(source *MgmtService, req *servers.HTTPRequest, next func() *servers.HTTPResponse) *servers.HTTPResponse {
		if checkFn(source, req) {
			sourceOut = source
			reqOut = req
			waitGrp.Done()
		}

		return next()
	})

	waitGrp.Wait()

	if panicErr != nil {
		panic(panicErr)
	}

	return sourceOut, reqOut
}
