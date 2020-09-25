package checks

import (
	"log"
	"testing"

	"github.com/couchbaselabs/gocaves/checks"
	"github.com/couchbaselabs/gocaves/checksuite"
)

func TestTestRunGroup(t *testing.T) {
	checksuite.RegisterCheckFuncs()

	rg, err := checks.NewTestRunGroup()
	if err != nil {
		t.Fatalf("failed to create run group: %s", err)
	}
	log.Printf("Run Group: %+v", rg)

	c, err := rg.StartTest("kv/crud/SetGet")
	if err != nil {
		t.Fatalf("failed to start test: %s", err)
	}
	log.Printf("Test: %+v", c)
}
