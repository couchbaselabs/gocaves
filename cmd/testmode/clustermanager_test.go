package testmode

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestClusterManagerLoadSample(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "sample.zip")
	out, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("failed to create archive: %v", err)
	}
	archive := zip.NewWriter(out)
	w, err := archive.Create("sample/docs/doc.json")
	if err != nil {
		t.Fatalf("failed to add document: %v", err)
	}
	if _, err := w.Write([]byte(`{}`)); err != nil {
		t.Fatalf("failed to write document: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("failed to close archive: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("failed to close archive file: %v", err)
	}

	var m clusterManager
	if _, err := m.NewCluster("c1"); err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	if err := m.LoadSample("missing", "default", zipPath); err == nil {
		t.Errorf("expected an error for an unknown cluster")
	}
	if err := m.LoadSample("c1", "missing", zipPath); err == nil {
		t.Errorf("expected an error for an unknown bucket")
	}
	if err := m.LoadSample("c1", "default", zipPath); err != nil {
		t.Errorf("failed to load sample: %v", err)
	}
}
