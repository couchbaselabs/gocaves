package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
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

	_, err = cluster.AddBucket(mock.NewBucketOptions{
		Name:        "memd",
		Type:        mock.BucketTypeMemcached,
		NumReplicas: 0,
	})
	if err != nil {
		return nil, err
	}

	err = cluster.Users().UpsertUser(mockauth.UpsertUserOptions{
		Username: "Administrator",
		Password: "password",
		Roles:    []string{"admin"},
	})
	if err != nil {
		return nil, err
	}

	return cluster, nil
}
