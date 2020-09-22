package example

import (
	"log"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/cmd"
)

func fakeFunc() {
	// We secretely have a fake function which depends on the CAVES
	// command line such that build errors occur here rather than when
	// we try to actually start up CAVES...
	cmd.Start()
}

func TestBasic(t *testing.T) {
	gocb.SetLogger(gocb.DefaultStdioLogger())

	caves, err := newCavesClient()
	if err != nil {
		t.Fatalf("failed to setup caves: %s", err)
	}

	connStr, err := caves.GetConnStr()
	if err != nil {
		t.Fatalf("failed to get connstr: %s", err)
	}

	connStr = connStr + "?auth_mechanisms=PLAIN"

	log.Printf("got connection string: %s", connStr)

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

	bucket.WaitUntilReady(10*time.Second, nil)

	// Write a key to the bucket
	testDoc := map[string]interface{}{
		"foo": "bar",
	}
	mutRes, err := collection.Upsert("test-doc", testDoc, nil)
	if err != nil {
		t.Fatalf("Failed to upsert: %s", err)
	}
	log.Printf("MutRes: %+v", mutRes)

	// Get a key from the bucket
	doc, err := collection.Get("test-doc", nil)
	if err != nil {
		t.Fatalf("Failed to get: %s", err)
	}

	log.Printf("Doc: %+v", doc)
}
