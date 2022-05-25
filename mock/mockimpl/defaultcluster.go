package mockimpl

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
)

// NewDefaultCluster returns a new cluster configured with some defaults.
func NewDefaultCluster(port ...int) (mock.Cluster, error) {
	listenPort := 0

	// if force-port has been specified the listenPort needs to be changed
	if len(port) > 0 {
		listenPort = port[0]
	}

	cluster, err := NewCluster(mock.NewClusterOptions{
		InitialNode: mock.NewNodeOptions{ListenPort: &listenPort},
	})
	if err != nil {
		return nil, err
	}

	_, err = cluster.AddNode(mock.NewNodeOptions{ListenPort: &listenPort})
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
