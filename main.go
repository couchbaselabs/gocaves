package main

import (
	"flag"
	"log"
	"time"

	mock "github.com/couchbaselabs/gocaves/mock"
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

func createDefaultCluster() *mock.Cluster {
	cluster, err := mock.NewCluster(mock.NewClusterOptions{
		InitialNode: mock.NewNodeOptions{},
	})
	if err != nil {
		panic(err)
	}

	cluster.AddNode(mock.NewNodeOptions{})
	cluster.AddNode(mock.NewNodeOptions{})

	_, err = cluster.AddBucket(mock.NewBucketOptions{
		Name:        "default",
		Type:        mock.BucketTypeCouchbase,
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
