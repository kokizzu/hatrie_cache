package hatCache

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// PersistentSpaceStoreConfig selects the durable engine for one logical
// space. Each space owns one independent store and can therefore choose a
// different backend, format, and path.
type PersistentSpaceStoreConfig struct {
	Name    string
	Path    string
	Backend StorageBackend
	Format  StorageFormat
}

// PersistentSpaceStoreSet owns the persistent stores configured for a group of
// logical spaces. Store lookup is O(1); callers should retain the returned
// store for hot paths instead of resolving the name for every operation.
type PersistentSpaceStoreSet struct {
	mu     sync.RWMutex
	stores map[string]PersistentStore
	closed bool
}

// OpenPersistentSpaceStoreSet opens one independently configured persistent
// store per space. Empty configuration creates an empty set. Names and paths
// must be unique after trimming and path cleaning.
func OpenPersistentSpaceStoreSet(configs []PersistentSpaceStoreConfig) (*PersistentSpaceStoreSet, error) {
	seenNames := make(map[string]struct{}, len(configs))
	seenPaths := make(map[string]struct{}, len(configs))
	opened := make(map[string]PersistentStore, len(configs))
	closeOpened := func(cause error) (*PersistentSpaceStoreSet, error) {
		var closeErrs []error
		for name, store := range opened {
			if err := store.Close(); err != nil {
				closeErrs = append(closeErrs, fmt.Errorf("close space %q: %w", name, err))
			}
		}
		return nil, errors.Join(append([]error{cause}, closeErrs...)...)
	}

	for _, config := range configs {
		name := strings.TrimSpace(config.Name)
		if name == "" {
			return closeOpened(errors.New("hatriecache: persistent space name is required"))
		}
		if _, exists := seenNames[name]; exists {
			return closeOpened(fmt.Errorf("hatriecache: duplicate persistent space name %q", name))
		}
		path := strings.TrimSpace(config.Path)
		if path == "" {
			return closeOpened(fmt.Errorf("hatriecache: persistent space %q path is required", name))
		}
		canonicalPath, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return closeOpened(fmt.Errorf("hatriecache: canonicalize persistent space %q path: %w", name, err))
		}
		if _, exists := seenPaths[canonicalPath]; exists {
			return closeOpened(fmt.Errorf("hatriecache: duplicate persistent space path %q", canonicalPath))
		}

		backend := config.Backend
		if strings.TrimSpace(string(backend)) == "" {
			backend = StorageBackendAuto
		}
		if _, err := ParseStorageBackend(string(backend)); err != nil {
			return closeOpened(fmt.Errorf("persistent space %q: %w", name, err))
		}
		format := config.Format
		if strings.TrimSpace(string(format)) == "" {
			format = DefaultStorageFormat
		}
		store, err := OpenPersistentStoreWithFormat(path, backend, format)
		if err != nil {
			return closeOpened(fmt.Errorf("open persistent space %q: %w", name, err))
		}
		seenNames[name] = struct{}{}
		seenPaths[canonicalPath] = struct{}{}
		opened[name] = store
	}

	return &PersistentSpaceStoreSet{stores: opened}, nil
}

// Store returns the persistent store for name. The name is trimmed before
// lookup; false is returned for an unknown space or a closed set.
func (set *PersistentSpaceStoreSet) Store(name string) (PersistentStore, bool) {
	if set == nil {
		return nil, false
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	if set.closed {
		return nil, false
	}
	store, ok := set.stores[strings.TrimSpace(name)]
	return store, ok
}

// Names returns the configured space names in deterministic order.
func (set *PersistentSpaceStoreSet) Names() []string {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	if set.closed {
		return nil
	}
	names := make([]string, 0, len(set.stores))
	for name := range set.stores {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Close closes every configured store. It is safe to call more than once.
func (set *PersistentSpaceStoreSet) Close() error {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		return nil
	}
	set.closed = true
	stores := set.stores
	set.stores = nil
	set.mu.Unlock()

	names := make([]string, 0, len(stores))
	for name := range stores {
		names = append(names, name)
	}
	sort.Strings(names)
	var closeErrs []error
	for _, name := range names {
		if err := stores[name].Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close space %q: %w", name, err))
		}
	}
	return errors.Join(closeErrs...)
}
