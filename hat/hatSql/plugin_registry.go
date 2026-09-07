package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrPluginRegistryNil reports a nil plugin registry.
	ErrPluginRegistryNil = errors.New("plugin registry is nil")
	// ErrPluginInvalid reports a plugin without a stable name or version.
	ErrPluginInvalid = errors.New("plugin is invalid")
	// ErrPluginDuplicate reports a duplicate initial load.
	ErrPluginDuplicate = errors.New("plugin already exists")
	// ErrPluginVersionRequired reports a replacement or unload without an
	// expected current version.
	ErrPluginVersionRequired = errors.New("plugin expected version is required")
	// ErrPluginVersionConflict reports an optimistic version check failure.
	ErrPluginVersionConflict = errors.New("plugin version conflict")
	// ErrPluginVersionUnchanged reports a replacement with the same version.
	ErrPluginVersionUnchanged = errors.New("plugin version is unchanged")
	// ErrPluginNotFound reports an unload or metadata lookup for an unknown
	// plugin.
	ErrPluginNotFound = errors.New("plugin was not found")
)

type pluginEntry struct {
	plugin   Plugin
	metadata PluginMetadata
}

// PluginMetadata identifies one loaded plugin generation.
type PluginMetadata struct {
	Name       string
	Version    string
	Generation uint64
}

// PluginRegistry atomically selects named versioned plugins. It is an
// in-process registry: it does not load native shared objects or execute
// plugin code outside the Plugin interfaces.
type PluginRegistry struct {
	mu             sync.RWMutex
	plugins        map[string]pluginEntry
	nextGeneration uint64
}

// NewPluginRegistry creates an empty registry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{plugins: make(map[string]pluginEntry)}
}

// Load installs a plugin. An empty expectedVersion is valid only for an
// initial load. Replacements must name the currently loaded version, making a
// stale hot reload fail without changing the active plugin.
func (registry *PluginRegistry) Load(plugin Plugin, expectedVersion string) (PluginMetadata, error) {
	if registry == nil {
		return PluginMetadata{}, ErrPluginRegistryNil
	}
	name, version, err := normalizePlugin(plugin)
	if err != nil {
		return PluginMetadata{}, err
	}
	expectedVersion = strings.TrimSpace(expectedVersion)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.plugins == nil {
		registry.plugins = make(map[string]pluginEntry)
	}
	current, exists := registry.plugins[name]
	if !exists {
		if expectedVersion != "" {
			return PluginMetadata{}, ErrPluginVersionConflict
		}
		return registry.installLocked(name, version, plugin), nil
	}
	if expectedVersion == "" {
		return PluginMetadata{}, ErrPluginVersionRequired
	}
	if expectedVersion != current.metadata.Version {
		return PluginMetadata{}, ErrPluginVersionConflict
	}
	if version == current.metadata.Version {
		return PluginMetadata{}, ErrPluginVersionUnchanged
	}
	return registry.installLocked(name, version, plugin), nil
}

// Resolve returns the currently active plugin for name.
func (registry *PluginRegistry) Resolve(name string) (Plugin, bool) {
	if registry == nil {
		return nil, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	registry.mu.RLock()
	entry, found := registry.plugins[name]
	registry.mu.RUnlock()
	if !found {
		return nil, false
	}
	return entry.plugin, true
}

// Metadata returns metadata for the active plugin.
func (registry *PluginRegistry) Metadata(name string) (PluginMetadata, bool) {
	if registry == nil {
		return PluginMetadata{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return PluginMetadata{}, false
	}
	registry.mu.RLock()
	entry, found := registry.plugins[name]
	registry.mu.RUnlock()
	if !found {
		return PluginMetadata{}, false
	}
	return entry.metadata, true
}

// Snapshot returns active plugin metadata in deterministic name order.
func (registry *PluginRegistry) Snapshot() []PluginMetadata {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	snapshot := make([]PluginMetadata, 0, len(registry.plugins))
	for _, entry := range registry.plugins {
		snapshot = append(snapshot, entry.metadata)
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool { return snapshot[left].Name < snapshot[right].Name })
	return snapshot
}

// Unload removes a plugin only when expectedVersion matches its current
// version. The check prevents an older owner from removing a replacement.
func (registry *PluginRegistry) Unload(name, expectedVersion string) error {
	if registry == nil {
		return ErrPluginRegistryNil
	}
	name = strings.TrimSpace(name)
	expectedVersion = strings.TrimSpace(expectedVersion)
	if name == "" {
		return ErrPluginInvalid
	}
	if expectedVersion == "" {
		return ErrPluginVersionRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, found := registry.plugins[name]
	if !found {
		return ErrPluginNotFound
	}
	if entry.metadata.Version != expectedVersion {
		return ErrPluginVersionConflict
	}
	delete(registry.plugins, name)
	return nil
}

func (registry *PluginRegistry) installLocked(name, version string, plugin Plugin) PluginMetadata {
	registry.nextGeneration++
	metadata := PluginMetadata{Name: name, Version: version, Generation: registry.nextGeneration}
	registry.plugins[name] = pluginEntry{plugin: plugin, metadata: metadata}
	return metadata
}

func normalizePlugin(plugin Plugin) (string, string, error) {
	if plugin == nil {
		return "", "", ErrPluginInvalid
	}
	value := reflect.ValueOf(plugin)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return "", "", ErrPluginInvalid
		}
	}
	name := strings.TrimSpace(plugin.PluginName())
	version := strings.TrimSpace(plugin.PluginVersion())
	if name == "" || version == "" {
		return "", "", ErrPluginInvalid
	}
	return name, version, nil
}
