package mockimpl

// CollectionManifest represents one version of a collection manifest
type CollectionManifest struct {
	Rev         uint
	Collections map[string]map[string]uint32
}

// GetByID returns the scope name and collection name for a particular ID.  It
// returns two blank strings if the ID was not found.
func (m CollectionManifest) GetByID(collectionID uint32) (string, string) {
	return "", ""
}
