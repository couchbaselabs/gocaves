package example

import (
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/cmd"
)

func fakeFunc() {
	// We secretly have a fake function which depends on the CAVES
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

	t.Logf("got connection string: %s", connStr)

	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		SecurityConfig: gocb.SecurityConfig{
			AllowedSaslMechanisms: []gocb.SaslMechanism{gocb.PlainSaslMechanism},
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
	t.Logf("MutRes: %+v", mutRes)

	// Get a key from the bucket
	doc, err := collection.Get("test-doc", nil)
	if err != nil {
		t.Fatalf("Failed to get: %s", err)
	}

	t.Logf("Doc: %+v", doc)
}
