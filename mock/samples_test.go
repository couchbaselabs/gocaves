package mock_test

import (
	"archive/zip"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockimpl"
)

func writeSampleZip(t *testing.T, files map[string]string) string {
	t.Helper()

	zipPath := filepath.Join(t.TempDir(), "sample.zip")
	out, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	archive := zip.NewWriter(out)
	for name, content := range files {
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
	return zipPath
}

// expectDocument checks that key is stored in the vbucket an SDK hashes it to.
func expectDocument(t *testing.T, bucket mock.Bucket, scope, collection, key, value string) {
	t.Helper()

	_, collectionID, err := bucket.CollectionManifest().GetByName(scope, collection)
	if err != nil {
		t.Fatalf("collection %s.%s is missing: %v", scope, collection, err)
	}

	store := bucket.Store()
	vbID := uint((crc32.ChecksumIEEE([]byte(key))>>16)&0x7fff) % store.NumVbuckets()
	doc, err := store.Get(0, vbID, uint(collectionID), []byte(key))
	if err != nil {
		t.Fatalf("document %s is not in %s.%s, vbucket %d: %v", key, scope, collection, vbID, err)
	}
	if string(doc.Value) != value {
		t.Errorf("document %s: expected %s, got %s", key, value, doc.Value)
	}
	// SDK JSON transcoders require the JSON common flags.
	if doc.Flags != 0x02000000 {
		t.Errorf("document %s: expected flags 0x02000000, got 0x%08x", key, doc.Flags)
	}
}

func TestLoadSampleWithCollections(t *testing.T) {
	cluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	bucket := cluster.GetBucket("default")

	zipPath := writeSampleZip(t, map[string]string{
		"travel-sample/collection_manifest.json": `{"uid":"2","scopes":[
			{"name":"_default","uid":"0","collections":[{"name":"_default","uid":"0"}]},
			{"name":"inventory","uid":"8","collections":[{"name":"airline","uid":"8"}]}]}`,
		"travel-sample/docs/_default._default.landmark_1.json": `{"id":1}`,
		"travel-sample/docs/inventory.airline.airline_10.json": `{"id":10}`,
		"travel-sample/design_docs/indexes.json":               `[]`,
	})

	if err := mock.LoadSample(bucket, zipPath); err != nil {
		t.Fatalf("failed to load sample: %v", err)
	}
	expectDocument(t, bucket, "_default", "_default", "landmark_1", `{"id":1}`)
	expectDocument(t, bucket, "inventory", "airline", "airline_10", `{"id":10}`)

	// A second load replaces the documents instead of failing on them.
	if err := mock.LoadSample(bucket, zipPath); err != nil {
		t.Fatalf("failed to reload sample: %v", err)
	}
	store := bucket.Store()
	key := []byte("landmark_1")
	vbID := uint((crc32.ChecksumIEEE(key)>>16)&0x7fff) % store.NumVbuckets()
	doc, err := store.Get(0, vbID, 0, key)
	if err != nil {
		t.Fatalf("failed to read reloaded document: %v", err)
	}
	if doc.RevID != 2 {
		t.Errorf("expected the reloaded document at revision 2, got %d", doc.RevID)
	}
}

func TestLoadSampleWithoutManifest(t *testing.T) {
	cluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	bucket := cluster.GetBucket("default")

	zipPath := writeSampleZip(t, map[string]string{
		"beer-sample/docs/21st_amendment_brewery_cafe.json": `{"name":"21st Amendment"}`,
	})

	if err := mock.LoadSample(bucket, zipPath); err != nil {
		t.Fatalf("failed to load sample: %v", err)
	}
	expectDocument(t, bucket, "_default", "_default", "21st_amendment_brewery_cafe", `{"name":"21st Amendment"}`)
}

func TestLoadSampleDocumentNamedLikeManifest(t *testing.T) {
	cluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	bucket := cluster.GetBucket("default")

	// Without a manifest at the archive root, a document named
	// collection_manifest is an ordinary document.
	for _, name := range []string{"sample/docs/collection_manifest.json", "docs/collection_manifest.json"} {
		zipPath := writeSampleZip(t, map[string]string{name: `{"name":"example"}`})

		if err := mock.LoadSample(bucket, zipPath); err != nil {
			t.Fatalf("failed to load sample with %s: %v", name, err)
		}
		expectDocument(t, bucket, "_default", "_default", "collection_manifest", `{"name":"example"}`)
	}
}

func TestLoadSampleRejectsMisnamedDocument(t *testing.T) {
	cluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	zipPath := writeSampleZip(t, map[string]string{
		"travel-sample/collection_manifest.json": `{"scopes":[]}`,
		"travel-sample/docs/airline_10.json":     `{}`,
	})

	if err := mock.LoadSample(cluster.GetBucket("default"), zipPath); err == nil {
		t.Fatalf("expected an error for a document without scope and collection")
	}
}
