package main

import (
	"time"

	mock "github.com/couchbaselabs/gocaves/mock"
	//checksuite "github.com/couchbaselabs/gocaves/checksuite"
)

func main() {
	cluster, err := mock.NewCluster(mock.NewClusterOptions{
		InitialNodes: []mock.NewNodeOptions{
			mock.NewNodeOptions{},
		},
	})
	if err != nil {
		panic(err)
	}

	cluster.GetBucket("default")

	time.Sleep(60 * time.Second)

	//checksuite.RegisterCheckFuncs()
}
