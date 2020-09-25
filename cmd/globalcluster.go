package cmd

import (
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl"
)

type globalClusterManager struct {
	cluster mock.Cluster
}

func (m *globalClusterManager) Get() (mock.Cluster, error) {
	if m.cluster == nil {
		cluster, err := mockimpl.NewDefaultCluster()
		if err != nil {
			return nil, err
		}
		m.cluster = cluster
	}
	return m.cluster, nil
}

var globalCluster globalClusterManager
