package example

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	cavescli "github.com/couchbaselabs/gocaves/client"
	"github.com/google/uuid"
)

func TestLoadSample(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "travel-sample.zip")
	out, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	archive := zip.NewWriter(out)
	for name, content := range map[string]string{
		"travel-sample/collection_manifest.json":               `{"scopes":[{"name":"inventory","collections":[{"name":"airline"}]}]}`,
		"travel-sample/docs/inventory.airline.airline_10.json": `{"name":"40-Mile Air"}`,
	} {
		w, err := archive.Create(name)
		if err != nil {
			t.Fatalf("failed to add %s: %v", name, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("failed to close archive file: %v", err)
	}

	caves, err := cavescli.NewClient(cavescli.NewClientOptions{
		Path: "../../main.go",
	})
	if err != nil {
		t.Fatalf("failed to setup caves: %s", err)
	}
	defer func() {
		if err := caves.Shutdown(); err != nil {
			t.Errorf("failed to shut down caves: %s", err)
		}
	}()

	clusterID := uuid.New().String()
	connDetails, err := caves.CreateCluster(clusterID)
	if err != nil {
		t.Fatalf("failed to create cluster: %s", err)
	}
	if err := caves.AddBucketCluster(clusterID, "travel-sample", "couchbase", 1); err != nil {
		t.Fatalf("failed to add bucket: %s", err)
	}
	if err := caves.LoadSampleCluster(clusterID, "travel-sample", zipPath); err != nil {
		t.Fatalf("failed to load sample: %s", err)
	}
	if err := caves.LoadSampleCluster(clusterID, "missing", zipPath); err == nil {
		t.Errorf("expected an error for an unknown bucket")
	}

	cluster, err := gocb.Connect(connDetails.ConnStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	})
	if err != nil {
		t.Fatalf("failed to connect to caves cluster: %s", err)
	}
	// TestBasic sets the global gocb logger, which this cluster's goroutines
	// read until it is closed.
	defer func() {
		if err := cluster.Close(nil); err != nil {
			t.Errorf("failed to close cluster: %s", err)
		}
	}()

	bucket := cluster.Bucket("travel-sample")
	if err := bucket.WaitUntilReady(10*time.Second, nil); err != nil {
		t.Fatalf("failed to wait for bucket: %s", err)
	}

	res, err := bucket.Scope("inventory").Collection("airline").Get("airline_10", nil)
	if err != nil {
		t.Fatalf("failed to get loaded document: %s", err)
	}
	var airline struct {
		Name string `json:"name"`
	}
	if err := res.Content(&airline); err != nil {
		t.Fatalf("failed to decode loaded document: %s", err)
	}
	if airline.Name != "40-Mile Air" {
		t.Errorf("unexpected document %+v", airline)
	}
}
