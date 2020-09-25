package checks

import (
	"errors"
	"fmt"
	"log"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl"
)

// TestRunGroup represents a single run of all tests.
type TestRunGroup struct {
	defaultCluster mock.Cluster

	allTests    []*T
	runningTest *T
}

// NewTestRunGroup creates a new test run group for running tests.
func NewTestRunGroup() (*TestRunGroup, error) {
	defaultCluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		return nil, err
	}

	group := &TestRunGroup{
		defaultCluster: defaultCluster,
	}

	allChecks := getAllRegisteredChecks()
	for _, check := range allChecks {
		group.allTests = append(group.allTests, &T{
			parent: group,
			def:    check,
		})
	}

	return group, nil
}

// DefaultCluster returns the default cluster associated with this test run.
func (g *TestRunGroup) DefaultCluster() mock.Cluster {
	return g.defaultCluster
}

func (g *TestRunGroup) findTest(name string) *T {
	for _, test := range g.allTests {
		fqName := fmt.Sprintf("%s/%s", test.def.Group, test.def.Name)
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
func (g *TestRunGroup) StartTest(name string) (*TestStartedSpec, error) {
	test := g.findTest(name)
	if test == nil {
		log.Printf("could not find a test to start it: %s", name)
		return nil, errors.New("not found")
	}

	g.runningTest = test

	return test.Start()
}

// EndRunningTest will end whatever test is currently running.
func (g *TestRunGroup) EndRunningTest(result interface{}) {
	if g.runningTest == nil {
		log.Printf("attempted to end running test with no running test")
		return
	}

	g.runningTest.End(result)

	g.runningTest = nil
}

// End will end any currently running test, then the whole test group.
func (g *TestRunGroup) End() {
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
