package mock

// CollectionManifest represents one version of a collection manifest
type CollectionManifest struct {
	Rev         uint64
	Scopes      map[uint32]CollectionManifestScopeEntry
	Collections map[uint32]CollectionManifestCollectionEntry
}

type CollectionManifestScopeEntry struct {
	Name string
	Uid  uint32
}

type CollectionManifestCollectionEntry struct {
	Name     string
	Uid      uint32
	ScopeUid uint32
}

// GetByID returns the scope name and collection name for a particular ID.  It
// returns two blank strings if the ID was not found.
func (m CollectionManifest) GetByID(collectionID uint32) (string, string) {
	col, ok := m.Collections[collectionID]
	if !ok {
		return "", ""
	}
	sid := col.ScopeUid
	scope, ok := m.Scopes[sid]
	if !ok {
		return "", ""
	}

	return scope.Name, col.Name
}
