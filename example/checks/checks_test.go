package example

import (
	"log"
	"testing"
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/cmd"
	"github.com/google/uuid"
)

func fakeFunc() {
	// We secretly have a fake function which depends on the CAVES
	// command line such that build errors occur here rather than when
	// we try to actually start up CAVES...
	cmd.Start()
}

func TestBasic(t *testing.T) {
	gocb.SetLogger(gocb.DefaultStdioLogger())

	caves, err := cavescli.NewClient(cavescli.NewClientOptions{
		Path:          "../../main.go",
		ReportingAddr: "127.0.0.1:9659",
	})
	if err != nil {
		t.Fatalf("failed to setup caves: %s", err)
	}

	runID := uuid.New().String()

	connStr, err := caves.StartTesting(runID, "FakeSDK v3.0.3-df13221")
	if err != nil {
		t.Fatalf("failed to start testing: %s", err)
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

	bucket.WaitUntilReady(10*time.Second, nil)

	spec, err := caves.StartTest(runID, "kv/crud/SetGet")
	if err != nil {
		t.Fatalf("failed to start test: %s", err)
	}

	log.Printf("started test: %+v", spec)

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
	//*
	doc, err := collection.Get("test-doc", nil)
	if err != nil {
		t.Fatalf("Failed to get: %s", err)
	}

	t.Logf("Doc: %+v", doc)
	//*/

	log.Printf("ending test")

	err = caves.EndTest(runID)
	if err != nil {
		t.Fatalf("failed to end test: %s", err)
	}

	log.Printf("ended test")

	report, err := caves.EndTesting(runID)
	if err != nil {
		t.Fatalf("failed to end testing: %s", err)
	}

	log.Printf("ended testing:\n%+v", report)
}
