package mock

import (
	"errors"

	"github.com/google/uuid"
)

// Cluster represents an instance of a mock cluster
type Cluster struct {
	id              string
	enabledFeatures []ClusterFeature
	numVbuckets     uint

	buckets []*Bucket
	nodes   []*ClusterNode

	currentConfig []byte
}

// NewClusterOptions allows the specification of initial options for a new cluster.
type NewClusterOptions struct {
	EnabledFeatures []ClusterFeature
	NumVbuckets     uint
	InitialNodes    []NewNodeOptions
	InitialBuckets  []NewBucketOptions
}

// NewCluster instantiates a new cluster instance.
func NewCluster(opts NewClusterOptions) (*Cluster, error) {
	if len(opts.InitialNodes) == 0 {
		return nil, errors.New("must start with at least 1 node")
	}

	cluster := &Cluster{
		id:              uuid.New().String(),
		enabledFeatures: opts.EnabledFeatures,
		numVbuckets:     opts.NumVbuckets,
		buckets:         nil,
		nodes:           nil,
	}

	for _, nodeOpts := range opts.InitialNodes {
		cluster.AddNode(nodeOpts)
	}

	for _, bucketOpts := range opts.InitialBuckets {
		cluster.AddBucket(bucketOpts)
	}

	return cluster, nil
}

// AddNode will add a new node to a cluster.
func (c *Cluster) AddNode(opts NewNodeOptions) (*ClusterNode, error) {
	node, err := NewClusterNode(c, opts)
	if err != nil {
		return nil, err
	}

	c.nodes = append(c.nodes, node)

	return node, nil
}

// AddBucket will add a new bucket to a cluster.
func (c *Cluster) AddBucket(opts NewBucketOptions) (*Bucket, error) {
	return nil, nil
}

// GetBucket will return a specific bucket from the cluster.
func (c *Cluster) GetBucket(name string) *Bucket {
	for _, bucket := range c.buckets {
		if bucket.Name() == name {
			return bucket
		}
	}
	return nil
}

// IsFeatureEnabled will indicate whether this cluster has a specific feature enabled.
func (c *Cluster) IsFeatureEnabled(feature ClusterFeature) bool {
	for _, supportedFeature := range c.enabledFeatures {
		if supportedFeature == feature {
			return true
		}
	}

	return false
}

func (c *Cluster) updateConfig() {
	c.currentConfig = nil
}
