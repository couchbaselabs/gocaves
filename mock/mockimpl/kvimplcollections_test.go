package mockimpl

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestMutationInDroppedScopeFails(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)

	manifest := c.cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}
	if _, err := manifest.AddCollection("app", "users", 0); err != nil {
		t.Fatalf("failed to add collection: %v", err)
	}

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte("app.users")})
	collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

	set := func() *memd.Packet {
		return c.Do(&memd.Packet{
			Command:      memd.CmdSet,
			Vbucket:      c.vbID,
			CollectionID: collectionID,
			Key:          []byte("user"),
			Value:        []byte(`{}`),
			Extras:       make([]byte, 8),
		})
	}

	if resp := set(); resp.Status != memd.StatusSuccess {
		t.Fatalf("set before drop failed with status 0x%02x", uint16(resp.Status))
	}

	manifestUID, err := manifest.DropScope("app")
	if err != nil {
		t.Fatalf("failed to drop scope: %v", err)
	}

	resp = set()
	if resp.Status != memd.StatusCollectionUnknown {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusCollectionUnknown), uint16(resp.Status))
	}
	var body struct {
		ManifestUID string `json:"manifest_uid"`
	}
	if err := json.Unmarshal(resp.Value, &body); err != nil {
		t.Fatalf("error body %q is not JSON: %v", resp.Value, err)
	}
	if body.ManifestUID != fmt.Sprintf("%x", manifestUID) {
		t.Fatalf("expected manifest_uid %x, got %q", manifestUID, body.ManifestUID)
	}

	resp = c.Do(&memd.Packet{Command: memd.CmdCollectionsGetID, Value: []byte("app.users")})
	if resp.Status != memd.StatusScopeUnknown {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusScopeUnknown), uint16(resp.Status))
	}

	// A second reply to GET_COLLECTION_ID would arrive here instead of the
	// GET response, and Do fails on the opaque mismatch.
	c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("user")})
}

func TestUnknownCollectionDuringManifestUpdates(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureCollections)
	manifest := c.cluster.GetBucket("default").CollectionManifest()
	if _, err := manifest.AddScope("app"); err != nil {
		t.Fatalf("failed to add scope: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan error)
	// Registered before the requests, so the worker stops even if one of
	// them fails the test. Cleanups run last-in first-out, so it stops
	// before the connection and cluster close.
	t.Cleanup(func() {
		close(stop)
		if err := <-done; err != nil {
			t.Errorf("failed to add collection: %v", err)
		}
	})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if _, err := manifest.AddCollection("app", fmt.Sprintf("c%d", i), 0); err != nil {
				done <- err
				return
			}
		}
	}()

	for i := 0; i < 50; i++ {
		resp := c.Do(&memd.Packet{
			Command:      memd.CmdGet,
			Vbucket:      c.vbID,
			CollectionID: 0xffff,
			Key:          []byte("doc"),
		})
		if resp.Status != memd.StatusCollectionUnknown {
			t.Errorf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusCollectionUnknown), uint16(resp.Status))
		}
	}
}
