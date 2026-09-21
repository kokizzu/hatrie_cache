package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// MaterializedViewPointLookupValueFunc converts a SQL equality literal into
// the canonical key used by a MaterializedViewPointLookupDefinition. It must
// agree with the row key function for every value it accepts; returning an
// error deliberately falls back to a complete source scan.
type MaterializedViewPointLookupValueFunc func(interface{}) (string, error)

// MaterializedViewPointLookupSourceDefinition describes how one maintained
// point lookup is exposed to the SQL planner as an EXTERNAL source. The
// default source name is EXTERNAL and the default source key is the indexed
// materialized-view name.
type MaterializedViewPointLookupSourceDefinition struct {
	IndexName    string
	SourceName   string
	SourceKey    string
	Field        string
	ValueKey     MaterializedViewPointLookupValueFunc
	CollectStats bool
}

// MaterializedViewPointLookupResolverStats contains optional planner counters
// for one materialized point lookup source. Counters are collected only when
// CollectStats is enabled in the source definition.
type MaterializedViewPointLookupResolverStats struct {
	SourceScans       uint64
	PointLookups      uint64
	PointLookupHits   uint64
	PointLookupMisses uint64
	LookupFallbacks   uint64
}

// MaterializedViewPointLookupResolver exposes one maintained materialized
// point lookup through SourceResolver and LookupSourceResolver. The SQL
// executor automatically chooses the point arrangement for a supported
// equality predicate and otherwise uses ResolveSQLSource for a full scan.
type MaterializedViewPointLookupResolver struct {
	views        *MaterializedViews
	viewName     string
	indexName    string
	sourceName   string
	sourceKey    string
	field        string
	valueKey     MaterializedViewPointLookupValueFunc
	collectStats bool

	sourceScans       atomic.Uint64
	pointLookups      atomic.Uint64
	pointLookupHits   atomic.Uint64
	pointLookupMisses atomic.Uint64
	lookupFallbacks   atomic.Uint64
}

// NewMaterializedViewPointLookupResolver creates a planner adapter for one
// maintained point lookup. The view and index must exist at construction;
// subsequent refreshes are observed atomically by both scan and point paths.
func NewMaterializedViewPointLookupResolver(views *MaterializedViews, definition MaterializedViewPointLookupSourceDefinition) (*MaterializedViewPointLookupResolver, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized point lookup resolver views are nil")
	}
	definition.IndexName = strings.TrimSpace(definition.IndexName)
	definition.SourceName = strings.TrimSpace(definition.SourceName)
	definition.SourceKey = strings.TrimSpace(definition.SourceKey)
	definition.Field = strings.TrimSpace(definition.Field)
	if definition.IndexName == "" {
		return nil, fmt.Errorf("materialized point lookup resolver index name is required")
	}
	if definition.Field == "" {
		return nil, fmt.Errorf("materialized point lookup resolver field is required")
	}
	if definition.ValueKey == nil {
		return nil, fmt.Errorf("materialized point lookup resolver value key function is required")
	}
	if definition.SourceName == "" {
		definition.SourceName = "EXTERNAL"
	}
	if !strings.EqualFold(definition.SourceName, "EXTERNAL") {
		return nil, fmt.Errorf("materialized point lookup resolver source name must be EXTERNAL")
	}
	definition.SourceName = "EXTERNAL"

	views.mu.RLock()
	index, exists := views.pointLookups[definition.IndexName]
	views.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexMissing, definition.IndexName)
	}
	if definition.SourceKey == "" {
		definition.SourceKey = index.definition.ViewName
	}
	if definition.SourceKey == "" {
		return nil, fmt.Errorf("materialized point lookup resolver source key is required")
	}
	return &MaterializedViewPointLookupResolver{
		views:        views,
		viewName:     index.definition.ViewName,
		indexName:    definition.IndexName,
		sourceName:   definition.SourceName,
		sourceKey:    definition.SourceKey,
		field:        definition.Field,
		valueKey:     definition.ValueKey,
		collectStats: definition.CollectStats,
	}, nil
}

// Stats returns a consistent snapshot of the optional planner counters.
func (resolver *MaterializedViewPointLookupResolver) Stats() MaterializedViewPointLookupResolverStats {
	if resolver == nil || !resolver.collectStats {
		return MaterializedViewPointLookupResolverStats{}
	}
	return MaterializedViewPointLookupResolverStats{
		SourceScans:       resolver.sourceScans.Load(),
		PointLookups:      resolver.pointLookups.Load(),
		PointLookupHits:   resolver.pointLookupHits.Load(),
		PointLookupMisses: resolver.pointLookupMisses.Load(),
		LookupFallbacks:   resolver.lookupFallbacks.Load(),
	}
}

// ResolveSQLSource supplies the complete materialized snapshot when the SQL
// planner cannot use the configured point arrangement.
func (resolver *MaterializedViewPointLookupResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if resolver == nil || !resolver.matches(name, key) {
		return nil, nil
	}
	if resolver.collectStats {
		resolver.sourceScans.Add(1)
	}
	view, found := resolver.views.Get(resolver.viewName)
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupViewMissing, resolver.viewName)
	}
	return view.Result.Rows, nil
}

// ResolveSQLExternalSource supplies the complete snapshot for EXTERNAL(key)
// scan fallback. The SQL executor uses this optional interface before it
// consults LookupSourceResolver for equality predicates.
func (resolver *MaterializedViewPointLookupResolver) ResolveSQLExternalSource(key string) ([]Row, error) {
	return resolver.ResolveSQLSource(resolver.sourceName, key)
}

// ResolveSQLLookupSource supplies candidate rows for one supported equality
// predicate. Returning available=false intentionally lets the existing SQL
// executor perform the complete source scan.
func (resolver *MaterializedViewPointLookupResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]Row, bool, error) {
	if resolver == nil || !resolver.matches(name, key) || !strings.EqualFold(strings.TrimSpace(field), resolver.field) {
		return nil, false, nil
	}
	if resolver.collectStats {
		resolver.pointLookups.Add(1)
	}
	canonical, err := resolver.valueKey(value)
	if err != nil {
		if resolver.collectStats {
			resolver.lookupFallbacks.Add(1)
		}
		return nil, false, nil
	}
	result, found, err := resolver.views.LookupPoint(resolver.indexName, canonical)
	if err != nil {
		if errors.Is(err, ErrMaterializedViewPointLookupIndexMissing) || errors.Is(err, ErrMaterializedViewPointLookupViewMissing) {
			if resolver.collectStats {
				resolver.lookupFallbacks.Add(1)
			}
			return nil, false, nil
		}
		return nil, true, err
	}
	if resolver.collectStats {
		if found {
			resolver.pointLookupHits.Add(1)
		} else {
			resolver.pointLookupMisses.Add(1)
		}
	}
	return result.Rows, true, nil
}

func (resolver *MaterializedViewPointLookupResolver) matches(name, key string) bool {
	return strings.EqualFold(strings.TrimSpace(name), resolver.sourceName) && strings.TrimSpace(key) == resolver.sourceKey
}
