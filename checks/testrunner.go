package checks

import (
	"errors"
	"fmt"
	"log"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl"
)

// TestResult represents the result of a test.
type TestResult struct {
	Name        string
	Description string
	Skipped     bool
	Success     bool
	Logs        []string
}

type pendingTest struct {
	Def    *Check
	Result *TestResult
}

// TestRunner represents a single run of all tests.
type TestRunner struct {
	defaultCluster mock.Cluster

	allTests    []*pendingTest
	runningTest *T
}

// NewTestRunner creates a new test run group for running tests.
func NewTestRunner() (*TestRunner, error) {
	defaultCluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		return nil, err
	}

	group := &TestRunner{
		defaultCluster: defaultCluster,
	}

	allChecks := getAllRegisteredChecks()
	for _, check := range allChecks {
		group.allTests = append(group.allTests, &pendingTest{
			Def: check,
			Result: &TestResult{
				Name:        fmt.Sprintf("%s/%s", check.Group, check.Name),
				Description: check.Description,
				Skipped:     true,
				Success:     false,
			},
		})
	}

	return group, nil
}

// DefaultCluster returns the default cluster associated with this test run.
func (g *TestRunner) DefaultCluster() mock.Cluster {
	return g.defaultCluster
}

func (g *TestRunner) findTest(name string) *pendingTest {
	for _, test := range g.allTests {
		fqName := fmt.Sprintf("%s/%s", test.Def.Group, test.Def.Name)
		if fqName == name {
			return test
		}
	}
	return nil
}

// TestStartedSpec represents all the information needed to run a test.
type TestStartedSpec struct {
	Cluster        mock.Cluster
	BucketName     string
	ScopeName      string
	CollectionName string
}

// StartTest will begin a test by name.
func (g *TestRunner) StartTest(name string) (*TestStartedSpec, error) {
	ptest := g.findTest(name)
	if ptest == nil {
		log.Printf("could not find a test to start it: %s", name)
		return nil, errors.New("not found")
	}

	test := &T{
		parent: g,
		ptest:  ptest,
		def:    ptest.Def,
	}

	g.runningTest = test

	return test.Start()
}

// EndRunningTest will end whatever test is currently running.
func (g *TestRunner) EndRunningTest(result interface{}) {
	if g.runningTest == nil {
		log.Printf("attempted to end running test with no running test")
		return
	}

	test := g.runningTest
	test.End(result)

	resultObj := test.ptest.Result
	resultObj.Skipped = false
	resultObj.Success = test.wasSuccess
	resultObj.Logs = test.logMsgs

	g.runningTest = nil
}

// End will end any currently running test, then the whole test group.
func (g *TestRunner) End() {
	log.Printf("TEST RUN GROUP ENDING:")
	for _, test := range g.allTests {
		log.Printf("  TEST: %+v", test)
	}

	if g.runningTest != nil {
		g.EndRunningTest(nil)
	}

	if g.defaultCluster != nil {
		//g.defaultCluster.Destroy()
		g.defaultCluster = nil
	}
}

// Results returns the results of this test run once it has ended.
func (g *TestRunner) Results() []*TestResult {
	results := make([]*TestResult, 0)
	for _, test := range g.allTests {
		results = append(results, test.Result)
	}
	return results
}
