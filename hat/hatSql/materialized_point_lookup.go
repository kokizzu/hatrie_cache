package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrMaterializedViewPointLookupIndexExists  = errors.New("materialized view point lookup index already exists")
	ErrMaterializedViewPointLookupIndexMissing = errors.New("materialized view point lookup index does not exist")
	ErrMaterializedViewPointLookupViewMissing  = errors.New("materialized view point lookup view does not exist")
)

// MaterializedViewPointLookupKeyFunc extracts the canonical key stored by a
// maintained materialized-view point lookup index. The row must be treated as
// read-only and the returned key must be deterministic.
type MaterializedViewPointLookupKeyFunc func(Row) (string, error)

// MaterializedViewPointLookupDefinition declares an exact point lookup index
// over one named materialized view. Duplicate keys are retained as multiple
// complete result rows.
type MaterializedViewPointLookupDefinition struct {
	Name     string
	ViewName string
	Key      MaterializedViewPointLookupKeyFunc
}

type materializedViewPointLookup struct {
	definition MaterializedViewPointLookupDefinition
	columns    []string
	rows       map[string][]Row
}

// CreatePointLookupIndex builds and publishes a maintained point lookup index
// from the current view snapshot. The index is refreshed atomically whenever
// its view is refreshed.
func (views *MaterializedViews) CreatePointLookupIndex(definition MaterializedViewPointLookupDefinition) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	var err error
	definition, err = normalizeMaterializedViewPointLookupDefinition(definition)
	if err != nil {
		return err
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	if views.pointLookups == nil {
		views.pointLookups = make(map[string]materializedViewPointLookup)
	}
	if _, exists := views.pointLookups[definition.Name]; exists {
		return fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexExists, definition.Name)
	}
	if _, exists := views.pointLookupBuilds[definition.Name]; exists {
		return fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupBuildInProgress, definition.Name)
	}
	view, exists := views.views[definition.ViewName]
	if !exists {
		return fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupViewMissing, definition.ViewName)
	}
	index, err := buildMaterializedViewPointLookup(definition, view.snapshot.Result)
	if err != nil {
		return err
	}
	views.pointLookups[definition.Name] = index
	return nil
}

// DropPointLookupIndex removes one maintained point lookup index.
func (views *MaterializedViews) DropPointLookupIndex(name string) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("materialized view point lookup index name is required")
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	if _, exists := views.pointLookups[name]; !exists {
		return fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexMissing, name)
	}
	delete(views.pointLookups, name)
	return nil
}

// LookupPoint returns all complete rows matching one indexed point key. The
// returned rows are independent copies and can be safely mutated by callers.
func (views *MaterializedViews) LookupPoint(indexName, key string) (QueryResult, bool, error) {
	if views == nil {
		return QueryResult{}, false, fmt.Errorf("materialized views are nil")
	}
	indexName = strings.TrimSpace(indexName)
	if indexName == "" {
		return QueryResult{}, false, fmt.Errorf("materialized view point lookup index name is required")
	}
	views.mu.RLock()
	index, exists := views.pointLookups[indexName]
	if !exists {
		views.mu.RUnlock()
		return QueryResult{}, false, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexMissing, indexName)
	}
	rows, found := index.rows[key]
	result := QueryResult{Columns: append([]string(nil), index.columns...)}
	if found {
		result.Rows = CloneRows(rows)
	}
	views.mu.RUnlock()
	return result, found, nil
}

func buildMaterializedViewPointLookup(definition MaterializedViewPointLookupDefinition, result QueryResult) (materializedViewPointLookup, error) {
	return buildMaterializedViewPointLookupWithProgress(context.Background(), definition, result, nil)
}
