package main

import (
	"flag"
	"log"
	"time"

	"github.com/couchbaselabs/gocaves/mockimpl"
	//checksuite "github.com/couchbaselabs/gocaves/checksuite"
)

/*
	--sdk
	Runs it as if its inside an SDK.  This doesn't start the web browser system.

	--mock-only
	Runs it for specific purposes of mocking.  Does not start the web browser
	system, and does not run the testing system.

*/

var sdkFlag = flag.Bool("sdk", false, "specifies we are running in a test-framework")
var mockOnlyFlag = flag.Bool("mock-only", false, "specifies only to use the mock")

func createDefaultCluster() *mockimpl.Cluster {
	cluster, err := mockimpl.NewCluster(mockimpl.NewClusterOptions{
		InitialNode: mockimpl.NewNodeOptions{},
	})
	if err != nil {
		panic(err)
	}

	cluster.AddNode(mockimpl.NewNodeOptions{})
	cluster.AddNode(mockimpl.NewNodeOptions{})

	_, err = cluster.AddBucket(mockimpl.NewBucketOptions{
		Name:        "default",
		Type:        mockimpl.BucketTypeCouchbase,
		NumReplicas: 1,
	})
	if err != nil {
		log.Printf("Failed to create bucket: %+v", err)
	}

	return cluster
}

func main() {
	flag.Parse()

	defaultCluster := createDefaultCluster()
	log.Printf("get default cluster: %+v", defaultCluster)

	time.Sleep(60 * time.Second)

	//checksuite.RegisterCheckFuncs()
}
