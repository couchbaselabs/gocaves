package mock

import (
	"errors"
	"sync"
)

// CollectionManifest represents one version of a collection manifest
type CollectionManifest struct {
	Rev         uint64
	Scopes      map[uint32]*collectionManifestScopeEntry
	Collections map[uint32]*collectionManifestCollectionEntry
	lock        sync.Mutex
}

// NewCollectionManifest creates a new collection manifest.
func NewCollectionManifest() *CollectionManifest {
	return &CollectionManifest{
		Rev: 0,
		Scopes: map[uint32]*collectionManifestScopeEntry{
			0: {
				Name: "_default",
				UID:  0,
			},
		},
		Collections: map[uint32]*collectionManifestCollectionEntry{
			0: {
				UID:  0,
				Name: "_default",
			},
		},
	}
}

type collectionManifestScopeEntry struct {
	Name string
	UID  uint32
}

type collectionManifestCollectionEntry struct {
	Name     string
	UID      uint32
	ScopeUID uint32
	MaxTTL   uint32
}

// CollectionManifestScope represents a scope in a collection manifest.
type CollectionManifestScope struct {
	Name        string
	UID         uint32
	Collections []CollectionManifestCollection
}

// CollectionManifestCollection represents a collection in a collection manifest.
type CollectionManifestCollection struct {
	Name   string
	UID    uint32
	MaxTTL uint32
}

// GetByID returns the scope name and collection name for a particular ID.  It
// returns two blank strings if the ID was not found.
func (m *CollectionManifest) GetByID(collectionID uint32) (string, string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	col, ok := m.Collections[collectionID]
	if !ok || col == nil {
		return "", ""
	}
	sid := col.ScopeUID
	scope, ok := m.Scopes[sid]
	if !ok || scope == nil {
		return "", ""
	}

	return scope.Name, col.Name
}

// GetByName retrieves a collection uid by scope and collection name.
func (m *CollectionManifest) GetByName(scope, collection string) (uint64, uint32, error) {
	m.lock.Lock()
	scopes := m.Scopes
	collections := m.Collections
	rev := m.Rev
	defer m.lock.Unlock()

	for _, scop := range scopes {
		if scop != nil && scop.Name == scope {
			for _, col := range collections {
				if col != nil && col.ScopeUID == scop.UID && col.Name == collection {
					return rev, col.UID, nil
				}
			}

			return 0, 0, ErrCollectionNotFound
		}
	}

	return 0, 0, ErrScopeNotFound
}

// AddCollection adds a new collection to the manifest.
func (m *CollectionManifest) AddCollection(scope, collection string, maxTTL uint32) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, scop := range m.Scopes {
		if scop != nil && scop.Name == scope {
			for _, col := range m.Collections {
				if col != nil && col.Name == collection && col.ScopeUID == scop.UID {
					return 0, ErrCollectionExists
				}
			}

			m.Rev++
			uid := uint32(len(m.Collections))
			newEntry := &collectionManifestCollectionEntry{
				Name:     collection,
				UID:      uid,
				ScopeUID: scop.UID,
				MaxTTL:   maxTTL,
			}

			m.Collections[uid] = newEntry
			return m.Rev, nil
		}
	}

	return 0, ErrScopeNotFound
}

// AddScope adds a new scope to the manifest.
func (m *CollectionManifest) AddScope(scope string) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, scop := range m.Scopes {
		if scop != nil && scop.Name == scope {
			return 0, ErrScopeExists
		}
	}

	m.Rev++
	uid := uint32(len(m.Scopes))
	newEntry := &collectionManifestScopeEntry{
		Name: scope,
		UID:  uid,
	}

	m.Scopes[uid] = newEntry
	return m.Rev, nil
}

// DropCollection removes a collection from the manifest.
func (m *CollectionManifest) DropCollection(scope, collection string) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, scop := range m.Scopes {
		if scop != nil && scop.Name == scope {
			for _, col := range m.Collections {
				if col != nil && col.ScopeUID == scop.UID && col.Name == collection {
					m.Rev++
					m.Collections[col.UID] = nil
					return m.Rev, nil
				}
			}
			return 0, ErrCollectionNotFound
		}
	}

	return 0, ErrScopeNotFound
}

// DropScope removes a scope from the manifest.
func (m *CollectionManifest) DropScope(scope string) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, scop := range m.Scopes {
		if scop != nil && scop.Name == scope {
			m.Rev++
			m.Scopes[scop.UID] = nil

			for _, col := range m.Collections {
				if col != nil && col.ScopeUID == scop.UID {
					m.Collections[col.UID] = nil
				}
			}

			return m.Rev, nil
		}
	}

	return 0, ErrScopeNotFound
}

// GetManifest gets the current manifest represented as a list of scopes, including collections, and the manifest uid.
func (m *CollectionManifest) GetManifest() (uint64, []CollectionManifestScope) {
	m.lock.Lock()
	scopes := m.Scopes
	collections := m.Collections
	uid := m.Rev
	m.lock.Unlock()

	collectionsByScope := make(map[uint32][]CollectionManifestCollection)
	for _, col := range collections {
		if col != nil {
			if _, ok := collectionsByScope[col.ScopeUID]; !ok {
				collectionsByScope[col.ScopeUID] = []CollectionManifestCollection{}
			}
			collectionsByScope[col.ScopeUID] = append(collectionsByScope[col.ScopeUID], CollectionManifestCollection{
				Name:   col.Name,
				UID:    col.UID,
				MaxTTL: col.MaxTTL,
			})
		}
	}

	var retScopes []CollectionManifestScope
	for _, scop := range scopes {
		if scop != nil {
			scope := CollectionManifestScope{
				UID:  scop.UID,
				Name: scop.Name,
			}

			cols := collectionsByScope[scope.UID]
			scope.Collections = append(scope.Collections, cols...)
			retScopes = append(retScopes, scope)
		}
	}

	return uid, retScopes
}

// A few errors that can be produced by collection manifest utilities.
var (
	ErrScopeExists        = errors.New("scope already exists")
	ErrCollectionExists   = errors.New("collection already exists")
	ErrScopeNotFound      = errors.New("scope not found")
	ErrCollectionNotFound = errors.New("collection not found")
)
