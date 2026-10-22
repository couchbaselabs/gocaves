package mock

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"
	"strings"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

// LoadSample loads a Couchbase Server sample archive, such as
// travel-sample.zip, into an existing bucket. Each scope and collection named
// in the archive's collection_manifest.json is created if missing, and each
// file under docs/ is stored as a document, replacing any document with the
// same key. With a manifest, a file is named
// <scope>.<collection>.<key>.json; without one, <key>.json goes to the default
// collection.
func LoadSample(bucket Bucket, zipPath string) (err error) {
	archive, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := archive.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	manifest := bucket.CollectionManifest()
	hasManifest := false
	for _, file := range archive.File {
		// A document under docs/ may have the manifest's name.
		if dir, name := path.Split(file.Name); name != "collection_manifest.json" || path.Base(dir) == "docs" {
			continue
		}
		data, err := readZipFile(file)
		if err != nil {
			return err
		}
		if err := createSampleCollections(manifest, data); err != nil {
			return err
		}
		hasManifest = true
	}

	store := bucket.Store()
	for _, file := range archive.File {
		dir, name := path.Split(file.Name)
		if path.Base(dir) != "docs" || !strings.HasSuffix(name, ".json") {
			continue
		}

		scopeName, collectionName := "_default", "_default"
		key := strings.TrimSuffix(name, ".json")
		if hasManifest {
			parts := strings.SplitN(key, ".", 3)
			if len(parts) != 3 {
				return fmt.Errorf("sample document %s is not named <scope>.<collection>.<key>.json", file.Name)
			}
			scopeName, collectionName, key = parts[0], parts[1], parts[2]
		}

		_, collectionID, err := manifest.GetByName(scopeName, collectionName)
		if err != nil {
			return fmt.Errorf("sample document %s: %v", file.Name, err)
		}

		value, err := readZipFile(file)
		if err != nil {
			return err
		}

		doc := &mockdb.Document{
			VbID:         vbucketForKey([]byte(key), store.NumVbuckets()),
			CollectionID: uint(collectionID),
			Key:          []byte(key),
			Value:        value,
			Datatype:     uint8(memd.DatatypeFlagJSON),
			Flags:        jsonCommonFlags,
			Cas:          mockdb.GenerateNewCas(store.Chrono().Now()),
		}
		_, err = store.Update(doc.VbID, doc.CollectionID, doc.Key, func(existing *mockdb.Document) (*mockdb.Document, error) {
			if existing != nil {
				// A replaced document continues the revision history of the
				// one it replaces.
				doc.RevID = existing.RevID
			}
			return doc, nil
		})
		if err != nil {
			return fmt.Errorf("sample document %s: %v", file.Name, err)
		}
	}

	return nil
}

func createSampleCollections(manifest *CollectionManifest, data []byte) error {
	var sample struct {
		Scopes []struct {
			Name        string `json:"name"`
			Collections []struct {
				Name string `json:"name"`
			} `json:"collections"`
		} `json:"scopes"`
	}
	if err := json.Unmarshal(data, &sample); err != nil {
		return fmt.Errorf("invalid collection_manifest.json: %v", err)
	}

	for _, scope := range sample.Scopes {
		if _, err := manifest.AddScope(scope.Name); err != nil && !errors.Is(err, ErrScopeExists) {
			return err
		}
		for _, collection := range scope.Collections {
			_, err := manifest.AddCollection(scope.Name, collection.Name, 0)
			if err != nil && !errors.Is(err, ErrCollectionExists) {
				return err
			}
		}
	}
	return nil
}

func readZipFile(file *zip.File) (data []byte, err error) {
	reader, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	return io.ReadAll(reader)
}

// jsonCommonFlags marks a document as JSON for SDK transcoders, which read the
// document flags rather than the protocol datatype.
const jsonCommonFlags = 0x02000000

// vbucketForKey maps a key to its vbucket the way SDKs do, so a loaded
// document is found where clients look for it.
func vbucketForKey(key []byte, numVbuckets uint) uint {
	return uint((crc32.ChecksumIEEE(key)>>16)&0x7fff) % numVbuckets
}
