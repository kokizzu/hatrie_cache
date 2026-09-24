package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrMaterializedViewBudgetExceeded indicates that a create or refresh would
// exceed the configured aggregate materialized-view storage budget.
var ErrMaterializedViewBudgetExceeded = errors.New("materialized view storage budget exceeded")

// MaterializedViewDefinition declares a query and the source keys that can
// invalidate its materialized result. Dependencies are explicit so callers can
// refresh only views affected by a source update.
type MaterializedViewDefinition struct {
	Name         string
	Query        string
	Dependencies []string
}

// MaterializedViewStatus describes one immutable materialized-view snapshot.
type MaterializedViewStatus struct {
	Name            string
	Dependencies    []string
	Revision        uint64
	RefreshedAt     time.Time
	IdempotencyKeys []string `json:"idempotency_keys,omitempty"`
}

// MaterializedViewRefreshMetadata carries optional source-write metadata to
// dependent views. Empty metadata preserves the legacy refresh path.
type MaterializedViewRefreshMetadata struct {
	IdempotencyKeys []string
}

// MaterializedViewsOptions bounds the aggregate logical result retained by a
// MaterializedViews registry. A zero limit is unlimited for compatibility.
type MaterializedViewsOptions struct {
	MaxRows  int
	MaxBytes int64
}

// MaterializedViewStorageUsage reports logical rows and encoded row bytes
// retained by a registry. Bytes are accounted when MaxBytes is configured.
type MaterializedViewStorageUsage struct {
	Rows     int
	Bytes    int64
	MaxRows  int
	MaxBytes int64
}

// MaterializedView combines a snapshot's status with its query result.
type MaterializedView struct {
	Status MaterializedViewStatus
	Result QueryResult
}

// MaterializedViews stores named query-result snapshots. It is safe for
// concurrent reads and refreshes.
type MaterializedViews struct {
	mu         sync.RWMutex
	views      map[string]materializedView
	dependents map[string][]string
	maxRows    int
	maxBytes   int64
	rows       int
	bytes      int64
}

type materializedView struct {
	definition     MaterializedViewDefinition
	snapshot       MaterializedView
	sourceVersions map[string]string
	collation      SQLCollation
	storedRows     int
	storedBytes    int64
}

// NewMaterializedViews creates an empty materialized-view registry.
func NewMaterializedViews() *MaterializedViews {
	views, _ := NewMaterializedViewsWithOptions(MaterializedViewsOptions{})
	return views
}

// NewMaterializedViewsWithOptions creates a registry with optional aggregate
// row and logical-byte admission limits. Limits are checked before a new
// snapshot is published; a rejected refresh keeps the prior snapshot.
func NewMaterializedViewsWithOptions(options MaterializedViewsOptions) (*MaterializedViews, error) {
	if options.MaxRows < 0 || options.MaxBytes < 0 {
		return nil, fmt.Errorf("materialized view storage limits must not be negative")
	}
	return &MaterializedViews{
		views:      make(map[string]materializedView),
		dependents: make(map[string][]string),
		maxRows:    options.MaxRows,
		maxBytes:   options.MaxBytes,
	}, nil
}

// Usage returns the registry's current aggregate logical storage accounting.
func (views *MaterializedViews) Usage() MaterializedViewStorageUsage {
	if views == nil {
		return MaterializedViewStorageUsage{}
	}
	views.mu.RLock()
	usage := MaterializedViewStorageUsage{
		Rows: views.rows, Bytes: views.bytes, MaxRows: views.maxRows, MaxBytes: views.maxBytes,
	}
	views.mu.RUnlock()
	return usage
}

func (views *MaterializedViews) storageBytes(result QueryResult) int64 {
	if views == nil || views.maxBytes <= 0 {
		return 0
	}
	return int64(sqlRowsBytes(result.Rows))
}

func (views *MaterializedViews) checkStorageBudget(rows int, bytes int64) error {
	if views.maxRows > 0 && rows > views.maxRows {
		return fmt.Errorf("%w: rows %d exceeds maximum %d", ErrMaterializedViewBudgetExceeded, rows, views.maxRows)
	}
	if views.maxBytes > 0 && bytes > views.maxBytes {
		return fmt.Errorf("%w: bytes %d exceeds maximum %d", ErrMaterializedViewBudgetExceeded, bytes, views.maxBytes)
	}
	return nil
}

// Create evaluates and publishes a new materialized view. The view name must
// be unique; use RefreshChanged to recompute an existing view.
func (views *MaterializedViews) Create(ctx context.Context, definition MaterializedViewDefinition, resolver SourceResolver, options QueryOptions) (MaterializedViewStatus, error) {
	definition, err := normalizeMaterializedViewDefinition(definition)
	if err != nil {
		return MaterializedViewStatus{}, err
	}
	if views == nil {
		return MaterializedViewStatus{}, fmt.Errorf("materialized views are nil")
	}
	result, sourceVersions, err := executeMaterializedViewQuery(ctx, definition.Query, definition.Dependencies, resolver, options)
	if err != nil {
		return MaterializedViewStatus{}, err
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	if views.views == nil {
		views.views = make(map[string]materializedView)
	}
	if views.dependents == nil {
		views.dependents = make(map[string][]string)
	}
	if _, exists := views.views[definition.Name]; exists {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q already exists", definition.Name)
	}
	storedRows := len(result.Rows)
	storedBytes := views.storageBytes(result)
	if err := views.checkStorageBudget(views.rows+storedRows, views.bytes+storedBytes); err != nil {
		return MaterializedViewStatus{}, err
	}
	status := MaterializedViewStatus{
		Name:         definition.Name,
		Dependencies: append([]string(nil), definition.Dependencies...),
		Revision:     1,
		RefreshedAt:  time.Now().UTC(),
	}
	views.views[definition.Name] = materializedView{
		definition:     definition,
		collation:      normalizedMaterializedViewCollation(options.Collation),
		sourceVersions: sourceVersions,
		snapshot: MaterializedView{
			Status: status,
			Result: cloneQueryResult(result),
		},
		storedRows:  storedRows,
		storedBytes: storedBytes,
	}
	views.rows += storedRows
	views.bytes += storedBytes
	for _, dependency := range definition.Dependencies {
		views.dependents[dependency] = append(views.dependents[dependency], definition.Name)
	}
	return cloneMaterializedViewStatus(status), nil
}

// Get returns an independent copy of the named materialized-view snapshot.
func (views *MaterializedViews) Get(name string) (MaterializedView, bool) {
	if views == nil {
		return MaterializedView{}, false
	}
	views.mu.RLock()
	view, exists := views.views[strings.TrimSpace(name)]
	views.mu.RUnlock()
	if !exists {
		return MaterializedView{}, false
	}
	return cloneMaterializedView(view.snapshot), true
}

// Drop removes one named materialized view and its retained storage accounting.
// The operation is idempotent only for nil registries; callers receive an error
// when the requested view does not exist.
func (views *MaterializedViews) Drop(name string) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("materialized view name is required")
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	view, exists := views.views[name]
	if !exists {
		return fmt.Errorf("materialized view %q does not exist", name)
	}
	delete(views.views, name)
	views.rows -= view.storedRows
	views.bytes -= view.storedBytes
	for dependency, dependents := range views.dependents {
		filtered := dependents[:0]
		for _, dependent := range dependents {
			if dependent != name {
				filtered = append(filtered, dependent)
			}
		}
		if len(filtered) == 0 {
			delete(views.dependents, dependency)
		} else {
			views.dependents[dependency] = filtered
		}
	}
	return nil
}

// RefreshChanged atomically publishes refreshed snapshots for views whose
// dependencies intersect changed. It leaves all prior snapshots untouched if
// any candidate query fails.
func (views *MaterializedViews) RefreshChanged(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions) ([]MaterializedViewStatus, error) {
	return views.RefreshChangedWithMetadata(ctx, changed, resolver, options, MaterializedViewRefreshMetadata{})
}

// RefreshChangedWithMetadata is RefreshChanged with optional metadata from
// the source mutation batch. The metadata is retained on each refreshed
// dependent-view status so consumers can correlate a published snapshot with
// its asynchronous source writes.
func (views *MaterializedViews) RefreshChangedWithMetadata(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions, metadata MaterializedViewRefreshMetadata) ([]MaterializedViewStatus, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
	metadataKeys := normalizeMaterializedViewIdempotencyKeys(metadata.IdempotencyKeys)
	changedSet := make(map[string]struct{}, len(changed))
	for _, dependency := range changed {
		if dependency = strings.TrimSpace(dependency); dependency != "" {
			changedSet[dependency] = struct{}{}
		}
	}
	if len(changedSet) == 0 {
		return nil, nil
	}

	views.mu.RLock()
	candidateNames := make(map[string]struct{})
	for dependency := range changedSet {
		for _, name := range views.dependents[dependency] {
			candidateNames[name] = struct{}{}
		}
	}
	candidates := make([]materializedView, 0, len(candidateNames))
	for name := range candidateNames {
		if view, exists := views.views[name]; exists {
			candidates = append(candidates, view)
		}
	}
	views.mu.RUnlock()
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].definition.Name < candidates[right].definition.Name
	})
	if len(candidates) == 0 {
		return nil, nil
	}

	results := make(map[string]QueryResult, len(candidates))
	versions := make(map[string]map[string]string, len(candidates))
	resultRows := make(map[string]int, len(candidates))
	resultBytes := make(map[string]int64, len(candidates))
	for _, candidate := range candidates {
		result, sourceVersions, err := executeMaterializedViewQuery(ctx, candidate.definition.Query, candidate.definition.Dependencies, resolver, options)
		if err != nil {
			return nil, fmt.Errorf("refresh materialized view %q: %w", candidate.definition.Name, err)
		}
		results[candidate.definition.Name] = cloneQueryResult(result)
		versions[candidate.definition.Name] = sourceVersions
		resultRows[candidate.definition.Name] = len(result.Rows)
		resultBytes[candidate.definition.Name] = views.storageBytes(result)
	}

	refreshedAt := time.Now().UTC()
	statuses := make([]MaterializedViewStatus, 0, len(candidates))
	views.mu.Lock()
	defer views.mu.Unlock()
	nextRows, nextBytes := views.rows, views.bytes
	for _, candidate := range candidates {
		current, exists := views.views[candidate.definition.Name]
		if !exists || !sameMaterializedViewDefinition(current.definition, candidate.definition) {
			continue
		}
		nextRows += resultRows[candidate.definition.Name] - current.storedRows
		nextBytes += resultBytes[candidate.definition.Name] - current.storedBytes
	}
	if err := views.checkStorageBudget(nextRows, nextBytes); err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		current, exists := views.views[candidate.definition.Name]
		if !exists || !sameMaterializedViewDefinition(current.definition, candidate.definition) {
			continue
		}
		current.snapshot.Result = results[candidate.definition.Name]
		current.sourceVersions = versions[candidate.definition.Name]
		current.collation = normalizedMaterializedViewCollation(options.Collation)
		current.snapshot.Status.Revision++
		current.snapshot.Status.RefreshedAt = refreshedAt
		current.snapshot.Status.IdempotencyKeys = append([]string(nil), metadataKeys...)
		current.storedRows = resultRows[candidate.definition.Name]
		current.storedBytes = resultBytes[candidate.definition.Name]
		views.views[candidate.definition.Name] = current
		statuses = append(statuses, cloneMaterializedViewStatus(current.snapshot.Status))
	}
	views.rows = nextRows
	views.bytes = nextBytes
	return statuses, nil
}

func normalizedMaterializedViewCollation(collation SQLCollation) SQLCollation {
	if collation == "" {
		return SQLCollationBinary
	}
	return collation
}

func materializedViewSourceVersions(resolver SourceResolver, dependencies []string) map[string]string {
	versions, ok := resolver.(SourceVersionResolver)
	if !ok || len(dependencies) == 0 {
		return nil
	}
	result := make(map[string]string, len(dependencies))
	for _, dependency := range dependencies {
		version, available, err := versions.SQLSourceVersion("CACHE", dependency)
		if err != nil || !available || version == "" {
			return nil
		}
		result[dependency] = version
	}
	return result
}

func executeMaterializedViewQuery(ctx context.Context, query string, dependencies []string, resolver SourceResolver, options QueryOptions) (QueryResult, map[string]string, error) {
	before := materializedViewSourceVersions(resolver, dependencies)
	queryOptions := options
	queryOptions.ProjectionCatalog = nil
	result, err := ExecuteQueryParameters(ctx, query, resolver, nil, queryOptions)
	if err != nil {
		return QueryResult{}, nil, err
	}
	after := materializedViewSourceVersions(resolver, dependencies)
	if before != nil && after != nil && !sameMaterializedViewSourceVersions(before, after) {
		return QueryResult{}, nil, fmt.Errorf("materialized view source changed during refresh")
	}
	return result, after, nil
}

func sameMaterializedViewSourceVersions(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func (views *MaterializedViews) lookupExact(query string, resolver SourceResolver, options QueryOptions) (QueryResult, bool) {
	if views == nil || resolver == nil || strings.TrimSpace(query) == "" || options.IndexHint.Mode != "" {
		return QueryResult{}, false
	}
	query = strings.TrimSpace(query)
	requestedCollation := normalizedMaterializedViewCollation(options.Collation)
	versions, versioned := resolver.(SourceVersionResolver)
	if !versioned {
		return QueryResult{}, false
	}
	views.mu.RLock()
	names := make([]string, 0, len(views.views))
	for name, view := range views.views {
		if view.definition.Query == query && view.collation == requestedCollation && len(view.sourceVersions) == len(view.definition.Dependencies) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		view := views.views[name]
		fresh := true
		for _, dependency := range view.definition.Dependencies {
			version, available, err := versions.SQLSourceVersion("CACHE", dependency)
			if err != nil || !available || version == "" || version != view.sourceVersions[dependency] {
				fresh = false
				break
			}
		}
		if fresh {
			result := cloneQueryResult(view.snapshot.Result)
			result.Plan = []ExplainStep{{Node: "PROJECTION HIT", Detail: name}}
			views.mu.RUnlock()
			return result, true
		}
	}
	views.mu.RUnlock()
	return QueryResult{}, false
}

func (views *MaterializedViews) explainProjections(query string, resolver SourceResolver, options QueryOptions) []ExplainProjection {
	if views == nil || strings.TrimSpace(query) == "" {
		return nil
	}
	query = strings.TrimSpace(query)
	requestedCollation := normalizedMaterializedViewCollation(options.Collation)
	versions, versioned := resolver.(SourceVersionResolver)
	views.mu.RLock()
	names := make([]string, 0, len(views.views))
	for name := range views.views {
		names = append(names, name)
	}
	sort.Strings(names)
	projections := make([]ExplainProjection, 0, len(names))
	selected := false
	for _, name := range names {
		view := views.views[name]
		candidate := ExplainProjection{
			Name:           name,
			EstimatedRows:  len(view.snapshot.Result.Rows),
			EstimatedBytes: sqlRowsBytes(view.snapshot.Result.Rows),
		}
		candidate.EstimatedIOBytes = candidate.EstimatedBytes
		switch {
		case options.IndexHint.Mode != "":
			candidate.RejectedReason = "index_hint_active"
		case view.definition.Query != query:
			candidate.RejectedReason = "query_not_exact"
		case view.collation != requestedCollation:
			candidate.RejectedReason = "collation_mismatch"
		case !versioned || len(view.sourceVersions) != len(view.definition.Dependencies):
			candidate.RejectedReason = "source_version_unavailable"
		default:
			fresh := true
			for _, dependency := range view.definition.Dependencies {
				version, available, err := versions.SQLSourceVersion("CACHE", dependency)
				if err != nil || !available || version == "" {
					candidate.RejectedReason = "source_version_unavailable"
					fresh = false
					break
				}
				if version != view.sourceVersions[dependency] {
					candidate.RejectedReason = "source_version_changed"
					fresh = false
					break
				}
			}
			if fresh {
				if selected {
					candidate.RejectedReason = "not_selected"
				} else {
					candidate.Selected = true
					selected = true
				}
			}
		}
		projections = append(projections, candidate)
	}
	views.mu.RUnlock()
	return projections
}

func normalizeMaterializedViewDefinition(definition MaterializedViewDefinition) (MaterializedViewDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Query = strings.TrimSpace(definition.Query)
	if definition.Name == "" {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view name is required")
	}
	if definition.Query == "" {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q query is required", definition.Name)
	}
	if len(definition.Dependencies) == 0 {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q dependencies are required", definition.Name)
	}
	dependencies := make([]string, 0, len(definition.Dependencies))
	seen := make(map[string]struct{}, len(definition.Dependencies))
	for _, dependency := range definition.Dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q has an empty dependency", definition.Name)
		}
		if _, exists := seen[dependency]; exists {
			return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q has duplicate dependency %q", definition.Name, dependency)
		}
		seen[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	definition.Dependencies = dependencies
	return definition, nil
}

func materializedViewDependsOn(definition MaterializedViewDefinition, changed map[string]struct{}) bool {
	for _, dependency := range definition.Dependencies {
		if _, exists := changed[dependency]; exists {
			return true
		}
	}
	return false
}

func sameMaterializedViewDefinition(left, right MaterializedViewDefinition) bool {
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

func cloneMaterializedView(view MaterializedView) MaterializedView {
	return MaterializedView{
		Status: cloneMaterializedViewStatus(view.Status),
		Result: cloneQueryResult(view.Result),
	}
}

func cloneMaterializedViewStatus(status MaterializedViewStatus) MaterializedViewStatus {
	status.Dependencies = append([]string(nil), status.Dependencies...)
	status.IdempotencyKeys = append([]string(nil), status.IdempotencyKeys...)
	return status
}

func normalizeMaterializedViewIdempotencyKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			normalized = append(normalized, key)
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func cloneQueryResult(result QueryResult) QueryResult {
	result.Columns = append([]string(nil), result.Columns...)
	result.Rows = CloneRows(result.Rows)
	result.Plan = cloneMaterializedExplainSteps(result.Plan)
	result.PlanSnapshot = cloneSQLPlanSnapshot(result.PlanSnapshot)
	if result.Stats != nil {
		stats := *result.Stats
		result.Stats = &stats
	}
	return result
}

func cloneMaterializedExplainSteps(steps []ExplainStep) []ExplainStep {
	cloned := make([]ExplainStep, len(steps))
	for index, step := range steps {
		cloned[index] = step
		cloned[index].Arrangements = cloneSQLArrangementMetadata(step.Arrangements)
		cloned[index].Projections = cloneExplainProjections(step.Projections)
		cloned[index].EstimatedRows = cloneMaterializedInt(step.EstimatedRows)
		cloned[index].ActualInputRows = cloneMaterializedInt(step.ActualInputRows)
		cloned[index].ActualOutputRows = cloneMaterializedInt(step.ActualOutputRows)
		cloned[index].ActualInputBytes = cloneMaterializedInt(step.ActualInputBytes)
		cloned[index].ActualOutputBytes = cloneMaterializedInt(step.ActualOutputBytes)
		cloned[index].EstimateErrorRows = cloneMaterializedInt(step.EstimateErrorRows)
		cloned[index].EstimateErrorPercent = cloneMaterializedFloat64(step.EstimateErrorPercent)
		cloned[index].ElapsedNanos = cloneMaterializedInt64(step.ElapsedNanos)
	}
	return cloned
}

func cloneExplainProjections(projections []ExplainProjection) []ExplainProjection {
	return append([]ExplainProjection(nil), projections...)
}

func cloneMaterializedInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneMaterializedInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneMaterializedFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
