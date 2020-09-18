package main

import (
	"log"
	"time"

	mock "github.com/couchbaselabs/gocaves/mock"
	//checksuite "github.com/couchbaselabs/gocaves/checksuite"
)

func main() {
	cluster, err := mock.NewCluster(mock.NewClusterOptions{
		InitialNode: mock.NewNodeOptions{},
	})
	if err != nil {
		panic(err)
	}

	cluster.AddNode(mock.NewNodeOptions{})
	cluster.AddNode(mock.NewNodeOptions{})

	newBucket, err := cluster.AddBucket(mock.NewBucketOptions{
		Name:        "default",
		Type:        mock.BucketTypeCouchbase,
		NumReplicas: 2,
	})
	if err != nil {
		log.Printf("Failed to create bucket: %+v", err)
	}
	log.Printf("Created Bucket: %p", newBucket)

	time.Sleep(60 * time.Second)

	//checksuite.RegisterCheckFuncs()
}
