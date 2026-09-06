package hatSql

import (
	"errors"
	"strings"
	"sync"
)

var ErrJSONSubcolumnPathInvalid = errors.New("hatriecache: JSON subcolumn path is invalid")

// JSONSubcolumn identifies one process-local shared JSON path.
type JSONSubcolumn struct {
	ID   uint32 `json:"id"`
	Path string `json:"path"`
}

// JSONSubcolumnRegistry interns repeated JSON paths so indexes and column
// metadata can refer to one shared path string. IDs are process-local and are
// assigned in first-seen order.
type JSONSubcolumnRegistry struct {
	mu     sync.RWMutex
	byPath map[string]uint32
	paths  []string
}

// NewJSONSubcolumnRegistry creates an empty shared-path registry.
func NewJSONSubcolumnRegistry() *JSONSubcolumnRegistry {
	return &JSONSubcolumnRegistry{byPath: make(map[string]uint32)}
}

// Intern returns the ID for path and reports whether a new path was created.
// Whitespace around a path is ignored; repeated calls for a known path do not
// allocate or create another metadata entry.
func (registry *JSONSubcolumnRegistry) Intern(path string) (uint32, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return 0, false, ErrJSONSubcolumnPathInvalid
	}
	if registry == nil {
		return 0, false, nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.byPath == nil {
		registry.byPath = make(map[string]uint32)
	}
	if id, ok := registry.byPath[path]; ok {
		return id, false, nil
	}
	if uint64(len(registry.paths)) >= uint64(^uint32(0)) {
		return 0, false, ErrJSONSubcolumnPathInvalid
	}
	id := uint32(len(registry.paths) + 1)
	registry.byPath[path] = id
	registry.paths = append(registry.paths, path)
	return id, true, nil
}

// Lookup returns the ID for a normalized path without creating it.
func (registry *JSONSubcolumnRegistry) Lookup(path string) (uint32, bool) {
	if registry == nil {
		return 0, false
	}
	path = strings.TrimSpace(path)
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return 0, false
	}
	registry.mu.RLock()
	id, ok := registry.byPath[path]
	registry.mu.RUnlock()
	return id, ok
}

// Path returns the normalized path for an ID.
func (registry *JSONSubcolumnRegistry) Path(id uint32) (string, bool) {
	if registry == nil || id == 0 {
		return "", false
	}
	registry.mu.RLock()
	index := uint64(id - 1)
	if index >= uint64(len(registry.paths)) {
		registry.mu.RUnlock()
		return "", false
	}
	path := registry.paths[index]
	registry.mu.RUnlock()
	return path, true
}

// Snapshot returns independently owned ID-ordered path metadata.
func (registry *JSONSubcolumnRegistry) Snapshot() []JSONSubcolumn {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	if len(registry.paths) == 0 {
		registry.mu.RUnlock()
		return nil
	}
	rows := make([]JSONSubcolumn, len(registry.paths))
	for index, path := range registry.paths {
		rows[index] = JSONSubcolumn{ID: uint32(index + 1), Path: path}
	}
	registry.mu.RUnlock()
	return rows
}
