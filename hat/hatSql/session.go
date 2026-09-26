package hatSql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// SQLSession owns temporary SQL sources and named result snapshots for one
// caller. It is safe for concurrent use and never mutates its base resolver.
type SQLSession struct {
	mu                  sync.RWMutex
	source              SourceResolver
	tables              map[string][]Row
	results             map[string][]Row
	views               map[string]sqlSessionView
	projections         *MaterializedViews
	projectionStore     SQLProjectionDefinitionStore
	projectionMu        sync.Mutex
	catalogVersion      uint64
	transactionSettings atomic.Pointer[SQLSessionTransactionSettings]
}

type sqlSessionView struct {
	source       string
	dependencies []string
}

func NewSQLSession(source SourceResolver) *SQLSession {
	session := &SQLSession{
		source:      source,
		tables:      map[string][]Row{},
		results:     map[string][]Row{},
		views:       map[string]sqlSessionView{},
		projections: NewMaterializedViews(),
	}
	settings := SQLSessionTransactionSettings{}
	session.transactionSettings.Store(&settings)
	return session
}

// SQLSessionOptions configures optional durable projection metadata. The
// default NewSQLSession constructor remains entirely in-memory.
type SQLSessionOptions struct {
	ProjectionDefinitionStore SQLProjectionDefinitionStore
	AutoRestoreProjections    bool
}

// NewSQLSessionWithOptions creates a session with optional durable projection
// metadata. AutoRestoreProjections rebuilds stored definitions against the
// supplied source before returning.
func NewSQLSessionWithOptions(source SourceResolver, options SQLSessionOptions) (*SQLSession, error) {
	session := NewSQLSession(source)
	session.SetProjectionDefinitionStore(options.ProjectionDefinitionStore)
	if options.AutoRestoreProjections {
		if options.ProjectionDefinitionStore == nil {
			return nil, fmt.Errorf("auto-restored SQL projections require a definition store")
		}
		if err := session.RestoreProjections(context.Background()); err != nil {
			return nil, err
		}
	}
	return session, nil
}

// SetProjectionDefinitionStore enables or disables durable projection
// definition persistence for subsequent projection DDL.
func (session *SQLSession) SetProjectionDefinitionStore(store SQLProjectionDefinitionStore) {
	if session == nil {
		return
	}
	session.projectionMu.Lock()
	defer session.projectionMu.Unlock()
	session.mu.Lock()
	session.projectionStore = store
	session.mu.Unlock()
}

func (session *SQLSession) projectionDefinitionStore() SQLProjectionDefinitionStore {
	if session == nil {
		return nil
	}
	session.mu.RLock()
	store := session.projectionStore
	session.mu.RUnlock()
	return store
}

// RestoreProjections rebuilds all definitions held by the configured store.
// The operation is explicit unless AutoRestoreProjections is enabled.
func (session *SQLSession) RestoreProjections(ctx context.Context) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.projectionMu.Lock()
	defer session.projectionMu.Unlock()
	store := session.projectionDefinitionStore()
	if store == nil {
		return fmt.Errorf("SQL projection definition store is not configured")
	}
	definitions, err := store.LoadSQLProjectionDefinitions(ctx)
	if err != nil {
		return err
	}
	projections := session.projectionCatalog()
	for _, definition := range definitions {
		if _, exists := projections.Get(definition.Name); exists {
			return fmt.Errorf("SQL projection %q already exists during restore", definition.Name)
		}
		if _, err := projections.Create(ctx, definition, session, QueryOptions{}); err != nil {
			return fmt.Errorf("restore SQL projection %q: %w", definition.Name, err)
		}
	}
	return nil
}

func (session *SQLSession) persistProjectionDefinitions(ctx context.Context) error {
	store := session.projectionDefinitionStore()
	if store == nil {
		return nil
	}
	return store.SaveSQLProjectionDefinitions(ctx, session.projectionCatalog().Definitions())
}

func (session *SQLSession) CreateTemporaryTable(name string, rows []Row) error {
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.mu.Lock()
	session.tables[key] = cloneSQLRows(rows)
	session.catalogVersion++
	session.mu.Unlock()
	return nil
}

func (session *SQLSession) DropTemporaryTable(name string) {
	_ = session.DropTemporaryTableChecked(name)
}

// DropTemporaryTableChecked removes a session-local table and reports a
// read-only rejection. DropTemporaryTable remains the compatibility wrapper
// for callers that use its historical no-error signature.
func (session *SQLSession) DropTemporaryTableChecked(name string) error {
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.mu.Lock()
	key := strings.ToLower(name)
	if _, exists := session.tables[key]; exists {
		delete(session.tables, key)
		session.catalogVersion++
	}
	session.mu.Unlock()
	return nil
}

func (session *SQLSession) StoreNamedResult(name string, result SQLQueryResult) error {
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.mu.Lock()
	session.results[key] = cloneSQLRows(result.Rows)
	session.mu.Unlock()
	return nil
}

// CreateView stores or replaces one session-local query definition. The
// mutation uses the same staged validation and publication boundary as a
// batch, with a single-change allocation fast path.
func (session *SQLSession) CreateView(name, source string) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	query := strings.TrimSpace(source)
	if query == "" {
		return fmt.Errorf("SQL view %q requires a query", name)
	}
	parsed, err := parseSQLQuery(query)
	if err != nil {
		return err
	}
	change := sqlSessionViewChange{
		name: key,
		view: sqlSessionView{source: query, dependencies: sqlQueryCacheDependencies(parsed)},
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if sqlSessionViewGraphHasCycleWithReplacement(session.views, change.name, change.view) {
		return fmt.Errorf("SQL view %q introduces a dependency cycle", name)
	}
	session.views[change.name] = change.view
	session.catalogVersion++
	return nil
}

// CreateProjection creates one source-version-guarded materialized projection
// for the session. Projection hits are selected automatically for exact query
// matches; an unversioned source is rejected so stale rows cannot be served.
func (session *SQLSession) CreateProjection(ctx context.Context, name, source string, options QueryOptions) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.projectionMu.Lock()
	defer session.projectionMu.Unlock()
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	query := strings.TrimSpace(source)
	if query == "" {
		return fmt.Errorf("SQL projection %q requires a query", name)
	}
	parsed, err := parseSQLQuery(query)
	if err != nil {
		return err
	}
	dependencies := sqlQueryCacheDependencies(parsed)
	if len(dependencies) == 0 {
		return fmt.Errorf("SQL projection %q requires at least one versioned CACHE source", name)
	}
	for _, dependency := range dependencies {
		version, available, err := session.SQLSourceVersion("CACHE", dependency)
		if err != nil {
			return fmt.Errorf("SQL projection %q source %q version: %w", name, dependency, err)
		}
		if !available || version == "" {
			return fmt.Errorf("SQL projection %q source %q does not provide a version", name, dependency)
		}
	}
	projectionOptions := options
	projectionOptions.ProjectionCatalog = nil
	projections := session.projectionCatalog()
	_, err = projections.Create(ctx, MaterializedViewDefinition{
		Name:         key,
		Query:        query,
		Dependencies: dependencies,
	}, session, projectionOptions)
	if err != nil {
		return err
	}
	if err := session.persistProjectionDefinitions(ctx); err != nil {
		_ = projections.Drop(key)
		return fmt.Errorf("persist SQL projection %q: %w", name, err)
	}
	return nil
}

// DropProjection removes one session-local materialized projection.
func (session *SQLSession) DropProjection(name string) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.projectionMu.Lock()
	defer session.projectionMu.Unlock()
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	projections := session.projectionCatalog()
	store := session.projectionDefinitionStore()
	if store == nil {
		return projections.Drop(key)
	}
	definitions := projections.Definitions()
	filtered := make([]MaterializedViewDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if definition.Name != key {
			filtered = append(filtered, definition)
		}
	}
	if err := store.SaveSQLProjectionDefinitions(context.Background(), filtered); err != nil {
		return fmt.Errorf("persist dropped SQL projection %q: %w", name, err)
	}
	if err := projections.Drop(key); err != nil {
		_ = store.SaveSQLProjectionDefinitions(context.Background(), definitions)
		return err
	}
	return nil
}

// RefreshProjection rebuilds one projection after its source versions advance.
// RefreshChanged is used so other projections sharing the same dependencies
// are refreshed atomically in the same maintenance pass.
func (session *SQLSession) RefreshProjection(ctx context.Context, name string, options QueryOptions) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := session.ensureSessionMutationAllowed(); err != nil {
		return err
	}
	session.projectionMu.Lock()
	defer session.projectionMu.Unlock()
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	projections := session.projectionCatalog()
	view, exists := projections.Get(key)
	if !exists {
		return fmt.Errorf("SQL projection %q does not exist", name)
	}
	refreshOptions := options
	refreshOptions.ProjectionCatalog = nil
	statuses, err := projections.RefreshChanged(ctx, view.Status.Dependencies, session, refreshOptions)
	if err != nil {
		return err
	}
	for _, status := range statuses {
		if status.Name == key {
			return nil
		}
	}
	return fmt.Errorf("SQL projection %q was not refreshed", name)
}

// SQLSourceVersion forwards source versions and assigns a session catalog
// version to temporary tables, so projection freshness is never inferred from
// an unversioned row slice.
func (session *SQLSession) SQLSourceVersion(name, key string) (string, bool, error) {
	if session == nil {
		return "", false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		key = strings.ToLower(strings.TrimSpace(key))
		_, tableExists := session.tables[key]
		_, resultExists := session.results[key]
		version := session.catalogVersion
		session.mu.RUnlock()
		if tableExists || resultExists {
			return fmt.Sprintf("session-%d-%s", version, key), true, nil
		}
	}
	if session.source == nil {
		return "", false, nil
	}
	versions, ok := session.source.(SourceVersionResolver)
	if !ok {
		return "", false, nil
	}
	return versions.SQLSourceVersion(name, key)
}

func (session *SQLSession) projectionCatalog() *MaterializedViews {
	session.mu.Lock()
	if session.projections == nil {
		session.projections = NewMaterializedViews()
	}
	projections := session.projections
	session.mu.Unlock()
	return projections
}

func (session *SQLSession) ResolveSQLSource(name, key string) ([]Row, error) {
	return session.resolveSQLSource(context.Background(), name, key)
}

// ResolveSQLSourceContext forwards the query context to the underlying source
// when it implements the optional context-aware materialized contract.
func (session *SQLSession) ResolveSQLSourceContext(ctx context.Context, name, key string) ([]Row, error) {
	return session.resolveSQLSource(ctx, name, key)
}

// ResolveSQLProjectedSource forwards the optional row projection contract to
// the external source after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLProjectedSource(name, key string, fields []string) ([]Row, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return nil, false, nil
	}
	projected, ok := session.source.(ProjectedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return projected.ResolveSQLProjectedSource(name, key, fields)
}

// ResolveSQLProjectedSourceContext forwards the context-aware row projection
// contract after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLProjectedSourceContext(ctx context.Context, name, key string, fields []string) ([]Row, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return nil, false, nil
	}
	projected, ok := session.source.(ContextProjectedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return projected.ResolveSQLProjectedSourceContext(ctx, name, key, fields)
}

func (session *SQLSession) resolveSQLSource(ctx context.Context, name, key string) ([]Row, error) {
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		if rows, exists := session.tables[strings.ToLower(key)]; exists {
			session.mu.RUnlock()
			return cloneSQLRows(rows), nil
		}
		if rows, exists := session.results[strings.ToLower(key)]; exists {
			session.mu.RUnlock()
			return cloneSQLRows(rows), nil
		}
		view, exists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if exists {
			result, err := session.Execute(ctx, view.source, nil, SQLQueryOptions{})
			if err != nil {
				return nil, err
			}
			return cloneSQLRows(result.Rows), nil
		}
	}
	if session.source == nil {
		return nil, nil
	}
	return resolveSQLSourceContext(ctx, session.source, name, key)
}

func (session *SQLSession) hasLocalSQLSource(name, key string) bool {
	if session == nil || !strings.EqualFold(name, "CACHE") {
		return false
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	key = strings.ToLower(key)
	_, tableExists := session.tables[key]
	_, resultExists := session.results[key]
	_, viewExists := session.views[key]
	return tableExists || resultExists || viewExists
}

// ResolveSQLColumnarSource forwards the optional columnar contract to the
// external source after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return ColumnarBatch{}, false, nil
	}
	columnar, ok := session.source.(ColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return columnar.ResolveSQLColumnarSource(name, key, fields)
}

// BorrowSQLColumnarSource forwards the optional immutable columnar contract
// to the external source after preserving session-local source precedence.
func (session *SQLSession) BorrowSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return ColumnarBatch{}, false, nil
	}
	borrowed, ok := session.source.(BorrowedColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return borrowed.BorrowSQLColumnarSource(name, key, fields)
}

// BorrowSQLColumnarSourceSegments forwards the optional immutable segmented
// columnar contract to the external source after preserving local precedence.
func (session *SQLSession) BorrowSQLColumnarSourceSegments(name, key string, fields []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return ColumnarBatch{}, nil, false, nil
	}
	segmented, ok := session.source.(SegmentedColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, nil, false, nil
	}
	return segmented.BorrowSQLColumnarSourceSegments(name, key, fields)
}

// BorrowSQLColumnarSourceOrder forwards the optional sorted ordinal contract
// to the external source after preserving session-local source precedence.
func (session *SQLSession) BorrowSQLColumnarSourceOrder(name, key string, fields []string, orderField string) ([]uint32, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return nil, false, nil
	}
	sorted, ok := session.source.(SortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrder(name, key, fields, orderField)
}

// BorrowSQLColumnarSourceOrderFields forwards the optional composite sorted
// ordinal contract to the external source after preserving local precedence.
func (session *SQLSession) BorrowSQLColumnarSourceOrderFields(name, key string, fields, orderFields []string) ([]uint32, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return nil, false, nil
	}
	sorted, ok := session.source.(CompositeSortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrderFields(name, key, fields, orderFields)
}

// BorrowSQLColumnarSourceOrderBy forwards the optional directed sorted ordinal
// contract to the external source after preserving local source precedence.
func (session *SQLSession) BorrowSQLColumnarSourceOrderBy(name, key string, fields, orderFields []string, descending []bool) ([]uint32, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return nil, false, nil
	}
	sorted, ok := session.source.(DirectedCompositeSortedColumnarSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return sorted.BorrowSQLColumnarSourceOrderBy(name, key, fields, orderFields, descending)
}

// PreferSQLColumnarSource forwards the optional columnar preference contract
// to the external source after preserving session-local source precedence.
func (session *SQLSession) PreferSQLColumnarSource(name, key string, fields []string) bool {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return false
	}
	preferred, ok := session.source.(ColumnarSourcePreferenceResolver)
	return ok && preferred.PreferSQLColumnarSource(name, key, fields)
}

// ResolveSQLSourcePartitions forwards the optional partition contract to the
// session's external source after giving session-local tables, results, and
// views the normal precedence.
func (session *SQLSession) ResolveSQLSourcePartitions(name, key string) ([]SQLSourcePartition, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		_, tableExists := session.tables[strings.ToLower(key)]
		_, resultExists := session.results[strings.ToLower(key)]
		_, viewExists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if tableExists || resultExists || viewExists {
			return nil, false, nil
		}
	}
	if session.source == nil {
		return nil, false, nil
	}
	partitioned, ok := session.source.(PartitionedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return partitioned.ResolveSQLSourcePartitions(name, key)
}

// ResolveSQLOrderedSourcePartitions forwards ordered physical partitions to
// the external source after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLOrderedSourcePartitions(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLSourcePartition, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		_, tableExists := session.tables[strings.ToLower(key)]
		_, resultExists := session.results[strings.ToLower(key)]
		_, viewExists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if tableExists || resultExists || viewExists {
			return nil, false, nil
		}
	}
	if session.source == nil {
		return nil, false, nil
	}
	ordered, ok := session.source.(PartitionedOrderedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return ordered.ResolveSQLOrderedSourcePartitions(name, key, field, desc, nullsFirst, nullsLast)
}

// ResolveSQLOrderedSourceRange forwards the optional ordered range contract
// after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLOrderedSourceRange(name, key, field string, desc, nullsFirst, nullsLast bool, operator string, value interface{}) ([]Row, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		_, tableExists := session.tables[strings.ToLower(key)]
		_, resultExists := session.results[strings.ToLower(key)]
		_, viewExists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if tableExists || resultExists || viewExists {
			return nil, false, nil
		}
	}
	if session.source == nil {
		return nil, false, nil
	}
	ordered, ok := session.source.(OrderedRangeSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return ordered.ResolveSQLOrderedSourceRange(name, key, field, desc, nullsFirst, nullsLast, operator, value)
}

// StreamSQLOrderedSourceRange forwards the optional ordered range stream
// contract after preserving session-local source precedence.
func (session *SQLSession) StreamSQLOrderedSourceRange(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, operator string, value interface{}, visit func(Row) error) (bool, error) {
	if session == nil {
		return false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		_, tableExists := session.tables[strings.ToLower(key)]
		_, resultExists := session.results[strings.ToLower(key)]
		_, viewExists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if tableExists || resultExists || viewExists {
			return false, nil
		}
	}
	if session.source == nil {
		return false, nil
	}
	ordered, ok := session.source.(OrderedRangeStreamSourceResolver)
	if !ok {
		return false, nil
	}
	return ordered.StreamSQLOrderedSourceRange(ctx, name, key, field, desc, nullsFirst, nullsLast, operator, value, visit)
}

// ResolveSQLSourcePartitionsForPredicate forwards the optional pruning
// contract after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLSourcePartitionsForPredicate(name, key string, predicate SQLPartitionPredicate) ([]SQLSourcePartition, bool, error) {
	if session == nil {
		return nil, false, nil
	}
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		_, tableExists := session.tables[strings.ToLower(key)]
		_, resultExists := session.results[strings.ToLower(key)]
		_, viewExists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if tableExists || resultExists || viewExists {
			return nil, false, nil
		}
	}
	if session.source == nil {
		return nil, false, nil
	}
	pruning, ok := session.source.(PartitionPruningSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return pruning.ResolveSQLSourcePartitionsForPredicate(name, key, predicate)
}

func sqlQueryCacheDependencies(query *sqlQuery) []string {
	seen := map[string]struct{}{}
	var collect func(*sqlQuery)
	add := func(source sqlSource) {
		if strings.EqualFold(source.kind, "CACHE") {
			seen[strings.ToLower(source.key)] = struct{}{}
		}
		if source.query != nil {
			collect(source.query)
		}
	}
	collect = func(query *sqlQuery) {
		if query == nil {
			return
		}
		if query.from != nil {
			add(*query.from)
		}
		for _, join := range query.joins {
			add(join.source)
		}
		for _, cte := range query.ctes {
			collect(cte.query)
		}
		for _, union := range query.unions {
			collect(union.query)
		}
	}
	collect(query)
	dependencies := make([]string, 0, len(seen))
	for dependency := range seen {
		dependencies = append(dependencies, dependency)
	}
	return dependencies
}

func (session *SQLSession) Execute(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if session == nil {
		return SQLQueryResult{}, ErrSQLSessionNil
	}
	ctx, cancel := session.transactionContext(ctx)
	defer cancel()
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE PROJECTION"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateProjection(ctx, name, query, options)
	}
	if name, matched, err := sqlSessionDropStatement(source, "DROP PROJECTION"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.DropProjection(name)
	}
	if name, matched, err := sqlSessionDropStatement(source, "REFRESH PROJECTION"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.RefreshProjection(ctx, name, options)
	}
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE OR REPLACE VIEW"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateView(name, query)
	}
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE VIEW"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateView(name, query)
	}
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE TEMP TABLE"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		result, err := ExecuteSQLQueryParameters(ctx, query, session, parameters, options)
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateTemporaryTable(name, result.Rows)
	}
	queryOptions := options
	if queryOptions.ProjectionCatalog == nil {
		queryOptions.ProjectionCatalog = session.projectionCatalog()
	}
	return ExecuteSQLQueryParameters(ctx, source, session, parameters, queryOptions)
}

func sqlSessionCreateStatement(source, prefix string) (string, string, bool, error) {
	trimmed := strings.TrimSpace(source)
	upper := strings.ToUpper(trimmed)
	prefixUpper := prefix + " "
	if !strings.HasPrefix(upper, prefixUpper) {
		return "", "", false, nil
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	separator := strings.Index(strings.ToUpper(rest), " AS ")
	if separator < 1 {
		return "", "", true, fmt.Errorf("%s requires a name and AS query", prefix)
	}
	name := strings.TrimSpace(rest[:separator])
	query := strings.TrimSpace(rest[separator+4:])
	if _, err := sessionObjectName(name); err != nil || query == "" {
		if err != nil {
			return "", "", true, err
		}
		return "", "", true, fmt.Errorf("%s requires a query after AS", prefix)
	}
	return name, query, true, nil
}

func sqlSessionDropStatement(source, prefix string) (string, bool, error) {
	trimmed := strings.TrimSpace(source)
	upper := strings.ToUpper(trimmed)
	prefixUpper := prefix + " "
	if !strings.HasPrefix(upper, prefixUpper) {
		return "", false, nil
	}
	name := strings.TrimSpace(trimmed[len(prefix):])
	if name == "" {
		return "", true, fmt.Errorf("%s requires a name", prefix)
	}
	key, err := sessionObjectName(name)
	if err != nil {
		return "", true, err
	}
	return key, true, nil
}

func sessionObjectName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !catalogIdentifier(name) {
		return "", fmt.Errorf("SQL session object name must be a simple identifier")
	}
	return strings.ToLower(name), nil
}
