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
				Uid:  0,
			},
		},
		Collections: map[uint32]*collectionManifestCollectionEntry{
			0: {
				Uid:  0,
				Name: "_default",
			},
		},
	}
}

type collectionManifestScopeEntry struct {
	Name string
	Uid  uint32
}

type collectionManifestCollectionEntry struct {
	Name     string
	Uid      uint32
	ScopeUid uint32
	MaxTTL   uint32
}

type CollectionManifestScope struct {
	Name        string
	Uid         uint32
	Collections []CollectionManifestCollection
}

type CollectionManifestCollection struct {
	Name   string
	Uid    uint32
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
	sid := col.ScopeUid
	scope, ok := m.Scopes[sid]
	if !ok || scope == nil {
		return "", ""
	}

	return scope.Name, col.Name
}

// AddCollection adds a new collection to the manifest.
func (m *CollectionManifest) AddCollection(scope, collection string, maxTTL uint32) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, col := range m.Collections {
		if col != nil && col.Name == collection {
			return 0, ErrCollectionExists
		}
	}
	for _, scop := range m.Scopes {
		if scop != nil && scop.Name == scope {
			m.Rev++
			uid := uint32(len(m.Collections))
			newEntry := &collectionManifestCollectionEntry{
				Name:     collection,
				Uid:      uid,
				ScopeUid: scop.Uid,
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
		Uid:  uid,
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
				if col != nil && col.Name == collection {
					m.Rev++
					m.Collections[col.Uid] = nil
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
			m.Scopes[scop.Uid] = nil

			for _, col := range m.Collections {
				if col != nil && col.ScopeUid == scop.Uid {
					m.Collections[col.Uid] = nil
				}
			}

			return m.Rev, nil
		}
	}

	return 0, ErrScopeNotFound
}

// GetAllScopes gets all scopes, including their collections, from the manifest.
func (m *CollectionManifest) GetAllScopes() (uint64, []CollectionManifestScope) {
	m.lock.Lock()
	scopes := m.Scopes
	collections := m.Collections
	uid := m.Rev
	m.lock.Unlock()

	collectionsByScope := make(map[uint32][]CollectionManifestCollection)
	for _, col := range collections {
		if col != nil {
			if _, ok := collectionsByScope[col.ScopeUid]; !ok {
				collectionsByScope[col.ScopeUid] = []CollectionManifestCollection{}
			}
			collectionsByScope[col.ScopeUid] = append(collectionsByScope[col.ScopeUid], CollectionManifestCollection{
				Name:   col.Name,
				Uid:    col.Uid,
				MaxTTL: col.MaxTTL,
			})
		}
	}

	var retScopes []CollectionManifestScope
	for _, scop := range scopes {
		if scop != nil {
			scope := CollectionManifestScope{
				Uid:  scop.Uid,
				Name: scop.Name,
			}

			cols := collectionsByScope[scope.Uid]
			for _, col := range cols {
				scope.Collections = append(scope.Collections, col)
			}
			retScopes = append(retScopes, scope)
		}
	}

	return uid, retScopes
}

var (
	ErrScopeExists        = errors.New("scope already exists")
	ErrCollectionExists   = errors.New("collection already exists")
	ErrScopeNotFound      = errors.New("scope not found")
	ErrCollectionNotFound = errors.New("collection not found")
)
