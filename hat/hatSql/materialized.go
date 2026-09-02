package hatSql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

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
	Name         string
	Dependencies []string
	Revision     uint64
	RefreshedAt  time.Time
}

// MaterializedView combines a snapshot's status with its query result.
type MaterializedView struct {
	Status MaterializedViewStatus
	Result QueryResult
}

// MaterializedViews stores named query-result snapshots. It is safe for
// concurrent reads and refreshes.
type MaterializedViews struct {
	mu    sync.RWMutex
	views map[string]materializedView
}

type materializedView struct {
	definition     MaterializedViewDefinition
	snapshot       MaterializedView
	sourceVersions map[string]string
	collation      SQLCollation
}

// NewMaterializedViews creates an empty materialized-view registry.
func NewMaterializedViews() *MaterializedViews {
	return &MaterializedViews{views: make(map[string]materializedView)}
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
	if _, exists := views.views[definition.Name]; exists {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q already exists", definition.Name)
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

// RefreshChanged atomically publishes refreshed snapshots for views whose
// dependencies intersect changed. It leaves all prior snapshots untouched if
// any candidate query fails.
func (views *MaterializedViews) RefreshChanged(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions) ([]MaterializedViewStatus, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
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
	candidates := make([]materializedView, 0, len(views.views))
	for _, view := range views.views {
		if materializedViewDependsOn(view.definition, changedSet) {
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
	for _, candidate := range candidates {
		result, sourceVersions, err := executeMaterializedViewQuery(ctx, candidate.definition.Query, candidate.definition.Dependencies, resolver, options)
		if err != nil {
			return nil, fmt.Errorf("refresh materialized view %q: %w", candidate.definition.Name, err)
		}
		results[candidate.definition.Name] = cloneQueryResult(result)
		versions[candidate.definition.Name] = sourceVersions
	}

	refreshedAt := time.Now().UTC()
	statuses := make([]MaterializedViewStatus, 0, len(candidates))
	views.mu.Lock()
	defer views.mu.Unlock()
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
		views.views[candidate.definition.Name] = current
		statuses = append(statuses, cloneMaterializedViewStatus(current.snapshot.Status))
	}
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
	return status
}

func cloneQueryResult(result QueryResult) QueryResult {
	result.Columns = append([]string(nil), result.Columns...)
	result.Rows = CloneRows(result.Rows)
	result.Plan = cloneMaterializedExplainSteps(result.Plan)
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
