package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"hatrie_cache/hat/hatSql"
)

// ErrUnknownSQLNamespace reports a query dispatched to an unregistered storage
// namespace.
var ErrUnknownSQLNamespace = errors.New("storage SQL namespace is not registered")

// SQLAdapter binds a persistent engine and relational source resolver to one
// namespace. The resolver is intentionally the hatSql contract, ensuring every
// storage adapter shares exactly the same parser, planner, and SQL semantics.
type SQLAdapter interface {
	Namespace() string
	SQLSourceResolver() hatSql.SourceResolver
	StorageEngine() Engine
}

// SQLNamespaceAdapter is the standard static SQLAdapter implementation. A
// HatTrie can be supplied as Resolver while a LevelDBStore or PebbleStore is
// supplied as Store.
type SQLNamespaceAdapter struct {
	NamespaceName string
	Store         Engine
	Resolver      hatSql.SourceResolver
}

func (adapter SQLNamespaceAdapter) Namespace() string { return adapter.NamespaceName }

func (adapter SQLNamespaceAdapter) SQLSourceResolver() hatSql.SourceResolver { return adapter.Resolver }

func (adapter SQLNamespaceAdapter) StorageEngine() Engine { return adapter.Store }

// SQLAdapterRegistry dispatches namespace-scoped queries to registered storage
// adapters. Registration is concurrency-safe; each execution still goes
// through the single hatSql execution layer.
type SQLAdapterRegistry struct {
	governor *hatSql.NamespaceQueryGovernor

	mu       sync.RWMutex
	adapters map[string]SQLAdapter
}

// NewSQLAdapterRegistry creates a registry and atomically validates every
// supplied adapter before returning it.
func NewSQLAdapterRegistry(governor *hatSql.NamespaceQueryGovernor, adapters ...SQLAdapter) (*SQLAdapterRegistry, error) {
	registry := &SQLAdapterRegistry{
		governor: governor,
		adapters: make(map[string]SQLAdapter, len(adapters)),
	}
	for _, adapter := range adapters {
		if err := registry.register(adapter); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// Register validates and registers a new namespace. Namespace names are exact
// identifiers so deployments may intentionally use names such as eu-west-1.
func (registry *SQLAdapterRegistry) Register(adapter SQLAdapter) error {
	if registry == nil {
		return fmt.Errorf("SQL adapter registry is required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.register(adapter)
}

func (registry *SQLAdapterRegistry) register(adapter SQLAdapter) error {
	if adapter == nil {
		return fmt.Errorf("SQL storage adapter is required")
	}
	namespace := strings.TrimSpace(adapter.Namespace())
	if namespace == "" {
		return fmt.Errorf("SQL storage adapter namespace is required")
	}
	if adapter.SQLSourceResolver() == nil {
		return fmt.Errorf("SQL storage adapter %q resolver is required", namespace)
	}
	store := adapter.StorageEngine()
	if _, err := Inspect(store); err != nil {
		return fmt.Errorf("SQL storage adapter %q: %w", namespace, err)
	}
	if _, exists := registry.adapters[namespace]; exists {
		return fmt.Errorf("SQL storage adapter namespace %q is already registered", namespace)
	}
	registry.adapters[namespace] = adapter
	return nil
}

func (registry *SQLAdapterRegistry) adapter(namespace string) (SQLAdapter, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL adapter registry is required")
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("SQL namespace is required")
	}
	registry.mu.RLock()
	adapter, ok := registry.adapters[namespace]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownSQLNamespace, namespace)
	}
	return adapter, nil
}

// Inspect returns the portable engine report for one registered namespace.
func (registry *SQLAdapterRegistry) Inspect(namespace string) (Inspection, error) {
	adapter, err := registry.adapter(namespace)
	if err != nil {
		return Inspection{}, err
	}
	return Inspect(adapter.StorageEngine())
}

// Execute evaluates source against namespace through the shared SQL executor.
// When governor is configured it applies the namespace resource policy before
// execution; otherwise no policy is silently invented.
func (registry *SQLAdapterRegistry) Execute(ctx context.Context, namespace, source string, parameters []interface{}, options hatSql.SQLQueryOptions) (hatSql.SQLQueryResult, error) {
	adapter, err := registry.adapter(namespace)
	if err != nil {
		return hatSql.SQLQueryResult{}, err
	}
	if registry.governor != nil {
		return registry.governor.Execute(ctx, namespace, source, adapter.SQLSourceResolver(), parameters, options)
	}
	return hatSql.ExecuteSQLQueryParameters(ctx, source, adapter.SQLSourceResolver(), parameters, options)
}
