package hooks

import (
	"errors"
	"sync"

	"github.com/couchbaselabs/gocaves/helpers/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
)

// MgmtHookExpect provides a nicer way to configure mgmt hooks.
type MgmtHookExpect struct {
	parent       *MgmtHookManager
	expectMethod string
	expectPath   *pathparse.Parser
}

// Method specifies a specific method which is expected.
func (e MgmtHookExpect) Method(method string) *MgmtHookExpect {
	e.expectMethod = method
	return &e
}

// Path specifies a specific path which is expected.
func (e MgmtHookExpect) Path(path string) *MgmtHookExpect {
	e.expectPath = pathparse.NewParser(path)
	return &e
}

// Handler specifies the handler to invoke if the expectations are met.
func (e MgmtHookExpect) Handler(fn func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse) *MgmtHookExpect {
	e.parent.Push(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
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
func (e MgmtHookExpect) Wait(checkFn func(mock.MgmtService, *mock.HTTPRequest) bool) (mock.MgmtService, *mock.HTTPRequest) {
	var sourceOut mock.MgmtService
	var reqOut *mock.HTTPRequest
	var panicErr error

	var waitGrp sync.WaitGroup
	waitGrp.Add(1)

	e.parent.pushDestroyer(func() {
		panicErr = errors.New("wait ended due to destroyed hook manager")
		waitGrp.Done()
	})
	e.Handler(func(source mock.MgmtService, req *mock.HTTPRequest, next func() *mock.HTTPResponse) *mock.HTTPResponse {
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
