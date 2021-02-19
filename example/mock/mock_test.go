package example

import (
	"testing"
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"
	"github.com/google/uuid"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/cmd"
)

func fakeFunc() { //nolint:deadcode,unused
	// We secretly have a fake function which depends on the CAVES
	// command line such that build errors occur here rather than when
	// we try to actually start up CAVES...
	cmd.Start()
}

func TestBasic(t *testing.T) {
	gocb.SetLogger(gocb.DefaultStdioLogger())

	caves, err := cavescli.NewClient(cavescli.NewClientOptions{
		Path: "../../main.go",
	})
	if err != nil {
		t.Fatalf("failed to setup caves: %s", err)
	}

	clusterID := uuid.New().String()
	connStr, err := caves.CreateCluster(clusterID)
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	t.Logf("got connection string: %s", connStr)

	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	})
	if err != nil {
		t.Fatalf("failed to connect to caves cluster: %s", err)
	}

	bucket := cluster.Bucket("default")
	collection := bucket.DefaultCollection()

	err = bucket.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		t.Fatalf("Failed to WaitUntilRead: %s", err)
	}

	// Write a key to the bucket
	testDoc := map[string]interface{}{
		"foo": "bar",
	}
	mutRes, err := collection.Upsert("test-doc", testDoc, nil)
	if err != nil {
		t.Fatalf("Failed to upsert: %s", err)
	}
	t.Logf("MutRes: %+v", mutRes)

	// Get a key from the bucket
	doc, err := collection.Get("test-doc", nil)
	if err != nil {
		t.Fatalf("Failed to get: %s", err)
	}

	t.Logf("Doc: %+v", doc)
}
