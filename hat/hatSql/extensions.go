package hatSql

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type SQLFeature string

const (
	FeatureVersionedFunctions SQLFeature = "versioned_functions"
	FeaturePlugins            SQLFeature = "plugins"
	FeatureVirtualSources     SQLFeature = "virtual_sources"
	FeatureCDC                SQLFeature = "cdc"
	FeatureIdempotency        SQLFeature = "idempotency"
)

type Capabilities map[SQLFeature]struct{}

func NewCapabilities(features ...SQLFeature) Capabilities {
	values := make(Capabilities, len(features))
	for _, feature := range features {
		values[feature] = struct{}{}
	}
	return values
}
func (values Capabilities) Has(feature SQLFeature) bool { _, ok := values[feature]; return ok }
func (values Capabilities) List() []SQLFeature {
	result := make([]SQLFeature, 0, len(values))
	for feature := range values {
		result = append(result, feature)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

type VersionedFunction struct {
	Package, Name, Version string
	Evaluate               func([]interface{}) (interface{}, error)
}
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]VersionedFunction
}

func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{functions: make(map[string]VersionedFunction)}
}
func (registry *FunctionRegistry) Register(function VersionedFunction) error {
	if registry == nil {
		return fmt.Errorf("function registry is required")
	}
	key, err := function.key()
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, ok := registry.functions[key]; ok {
		return fmt.Errorf("function %s already registered", key)
	}
	registry.functions[key] = function
	return nil
}
func (registry *FunctionRegistry) Resolve(pkg, name, version string) (VersionedFunction, bool) {
	if registry == nil {
		return VersionedFunction{}, false
	}
	registry.mu.RLock()
	value, ok := registry.functions[functionKey(pkg, name, version)]
	registry.mu.RUnlock()
	return value, ok
}
func (function VersionedFunction) key() (string, error) {
	if strings.TrimSpace(function.Package) == "" || strings.TrimSpace(function.Name) == "" || strings.TrimSpace(function.Version) == "" || function.Evaluate == nil {
		return "", fmt.Errorf("function package, name, version, and evaluator are required")
	}
	return functionKey(function.Package, function.Name, function.Version), nil
}
func functionKey(pkg, name, version string) string {
	return strings.ToLower(strings.TrimSpace(pkg)) + "/" + strings.ToLower(strings.TrimSpace(name)) + "@" + strings.TrimSpace(version)
}

// Plugin is a transport-neutral extension point. Implementations may expose
// any combination of custom source, index, or output encoder interfaces.
type Plugin interface {
	PluginName() string
	PluginVersion() string
}
type SourcePlugin interface {
	Plugin
	ResolveSQLSource(name, key string) ([]Row, error)
}
type IndexPlugin interface {
	Plugin
	ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error)
}
type OutputEncoderPlugin interface {
	Plugin
	EncodeSQLResult(QueryResult) ([]byte, string, error)
}
