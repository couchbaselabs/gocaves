package testmode

import (
	"errors"
	"time"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockimpl"
)

type namedCluster struct {
	Name string
	Mock mock.Cluster
}

type clusterManager struct {
	Clusters []*namedCluster
}

func (m *clusterManager) NewCluster(clusterID string) (*namedCluster, error) {
	mock, err := mockimpl.NewDefaultCluster()
	if err != nil {
		return nil, err
	}

	ncluster := &namedCluster{
		Name: clusterID,
		Mock: mock,
	}
	m.Clusters = append(m.Clusters, ncluster)

	return ncluster, nil
}

func (m *clusterManager) Get(clusterID string) *namedCluster {
	for _, run := range m.Clusters {
		if run.Name == clusterID {
			return run
		}
	}
	return nil
}

func (m *clusterManager) TimeTravel(clusterID string, amount time.Duration) error {
	cluster := m.Get(clusterID)
	if cluster == nil {
		return errors.New("invalid cluster id")
	}

	cluster.Mock.Chrono().TimeTravel(amount)

	return nil
}

func (m *clusterManager) RemoveCluster(clusterID string) error {
	ncluster := m.Get(clusterID)
	if ncluster == nil {
		return errors.New("invalid cluster id")
	}

	// TODO(brett19): Add support for destroying a mock
	//return ncluster.Cluster.Destroy()
	return nil
}

func (m *clusterManager) AddBucket(clusterID, name, typ string, replicas uint) error {
	ncluster := m.Get(clusterID)
	if ncluster == nil {
		return errors.New("invalid cluster id")
	}

	var bucketType mock.BucketType
	switch typ {
	case "couchbase":
		bucketType = mock.BucketTypeCouchbase
	case "memcached":
		bucketType = mock.BucketTypeMemcached
	}

	_, err := ncluster.Mock.AddBucket(mock.NewBucketOptions{
		Name:        name,
		Type:        bucketType,
		NumReplicas: replicas,
	})
	return err
}
