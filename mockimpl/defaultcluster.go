package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
)

// NewDefaultCluster returns a new cluster configured with some defaults.
func NewDefaultCluster() (mock.Cluster, error) {
	cluster, err := NewCluster(mock.NewClusterOptions{
		InitialNode: mock.NewNodeOptions{},
	})
	if err != nil {
		return nil, err
	}

	_, err = cluster.AddNode(mock.NewNodeOptions{})
	if err != nil {
		return nil, err
	}

	_, err = cluster.AddBucket(mock.NewBucketOptions{
		Name:        "default",
		Type:        mock.BucketTypeCouchbase,
		NumReplicas: 1,
	})
	if err != nil {
		return nil, err
	}

	return cluster, nil
}
