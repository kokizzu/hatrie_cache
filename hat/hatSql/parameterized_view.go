package hatSql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ParameterizedViewDefinition declares a named SQL query whose positional
// parameters are supplied when it is queried. Dependencies control targeted
// result invalidation after source changes.
type ParameterizedViewDefinition struct {
	Name         string
	Query        string
	Dependencies []string
}

// ParameterizedViews stores immutable query-result snapshots by view name and
// argument value. It is safe for concurrent queries, creates, and invalidation.
type ParameterizedViews struct {
	mu          sync.RWMutex
	definitions map[string]ParameterizedViewDefinition
	cache       map[string]QueryResult
}

// NewParameterizedViews creates an empty parameterized-view registry.
func NewParameterizedViews() *ParameterizedViews {
	return &ParameterizedViews{definitions: make(map[string]ParameterizedViewDefinition), cache: make(map[string]QueryResult)}
}

// Create installs definition. Existing names are rejected to keep invalidation
// ownership explicit; callers can create a new registry or invalidate first.
func (views *ParameterizedViews) Create(definition ParameterizedViewDefinition) error {
	definition, err := normalizeParameterizedViewDefinition(definition)
	if err != nil {
		return err
	}
	if views == nil {
		return fmt.Errorf("parameterized views are nil")
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	if _, exists := views.definitions[definition.Name]; exists {
		return fmt.Errorf("parameterized view %q already exists", definition.Name)
	}
	views.definitions[definition.Name] = definition
	return nil
}

// Query returns the cached result for name and parameters or executes and
// atomically installs a fresh immutable snapshot.
func (views *ParameterizedViews) Query(ctx context.Context, name string, parameters []interface{}, resolver SourceResolver, options QueryOptions) (QueryResult, error) {
	if views == nil {
		return QueryResult{}, fmt.Errorf("parameterized views are nil")
	}
	name = strings.TrimSpace(name)
	key, err := parameterizedViewCacheKey(name, parameters)
	if err != nil {
		return QueryResult{}, err
	}
	views.mu.RLock()
	definition, exists := views.definitions[name]
	if cached, ok := views.cache[key]; ok {
		views.mu.RUnlock()
		return cloneQueryResult(cached), nil
	}
	views.mu.RUnlock()
	if !exists {
		return QueryResult{}, fmt.Errorf("parameterized view %q does not exist", name)
	}
	result, err := ExecuteQueryParameters(ctx, definition.Query, resolver, parameters, options)
	if err != nil {
		return QueryResult{}, err
	}
	views.mu.Lock()
	if current, stillExists := views.definitions[name]; stillExists && sameParameterizedViewDefinition(current, definition) {
		if cached, ok := views.cache[key]; ok {
			views.mu.Unlock()
			return cloneQueryResult(cached), nil
		}
		views.cache[key] = cloneQueryResult(result)
	}
	views.mu.Unlock()
	return cloneQueryResult(result), nil
}

// Invalidate drops snapshots only for views whose dependencies intersect
// changed. It returns invalidated names in stable order.
func (views *ParameterizedViews) Invalidate(changed []string) []string {
	if views == nil {
		return nil
	}
	changedSet := make(map[string]struct{}, len(changed))
	for _, dependency := range changed {
		if dependency = strings.TrimSpace(dependency); dependency != "" {
			changedSet[dependency] = struct{}{}
		}
	}
	if len(changedSet) == 0 {
		return nil
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	invalidated := make([]string, 0)
	for name, definition := range views.definitions {
		if !parameterizedViewDependsOn(definition, changedSet) {
			continue
		}
		invalidated = append(invalidated, name)
		prefix := name + "\x00"
		for key := range views.cache {
			if strings.HasPrefix(key, prefix) {
				delete(views.cache, key)
			}
		}
	}
	sort.Strings(invalidated)
	return invalidated
}

func normalizeParameterizedViewDefinition(definition ParameterizedViewDefinition) (ParameterizedViewDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Query = strings.TrimSpace(definition.Query)
	if definition.Name == "" || definition.Query == "" {
		return ParameterizedViewDefinition{}, fmt.Errorf("parameterized view name and query are required")
	}
	if len(definition.Dependencies) == 0 {
		return ParameterizedViewDefinition{}, fmt.Errorf("parameterized view %q dependencies are required", definition.Name)
	}
	seen := make(map[string]struct{}, len(definition.Dependencies))
	dependencies := make([]string, 0, len(definition.Dependencies))
	for _, dependency := range definition.Dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return ParameterizedViewDefinition{}, fmt.Errorf("parameterized view %q has an empty dependency", definition.Name)
		}
		if _, exists := seen[dependency]; exists {
			return ParameterizedViewDefinition{}, fmt.Errorf("parameterized view %q has duplicate dependency %q", definition.Name, dependency)
		}
		seen[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	definition.Dependencies = dependencies
	return definition, nil
}

func parameterizedViewCacheKey(name string, parameters []interface{}) (string, error) {
	encoded, err := json.Marshal(parameters)
	if err != nil {
		return "", fmt.Errorf("encode parameters for view %q: %w", name, err)
	}
	return name + "\x00" + string(encoded), nil
}

func parameterizedViewDependsOn(definition ParameterizedViewDefinition, changed map[string]struct{}) bool {
	for _, dependency := range definition.Dependencies {
		if _, exists := changed[dependency]; exists {
			return true
		}
	}
	return false
}

func sameParameterizedViewDefinition(left, right ParameterizedViewDefinition) bool {
	if left.Name != right.Name || left.Query != right.Query || len(left.Dependencies) != len(right.Dependencies) {
		return false
	}
	for index := range left.Dependencies {
		if left.Dependencies[index] != right.Dependencies[index] {
			return false
		}
	}
	return true
}
