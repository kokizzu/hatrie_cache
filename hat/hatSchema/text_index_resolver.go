package hatSchema

import (
	"strings"

	"hatrie_cache/hat/hatSql"
)

// ResolveSQLTextProximitySource exposes the opt-in positional index to the
// SQL phrase/proximity planner. Returned rows are candidates; hatSql evaluates
// the complete predicate again before publishing results.
func (adapter SQLResolverAdapter) ResolveSQLTextProximitySource(name, key, field, query string, maxGap int) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			rows, available := source.lookupText(field, query, maxGap)
			if !available {
				return nil, false, nil
			}
			return sqlRows(rows), true, nil
		}
	}
	if indexed, ok := adapter.Base.(hatSql.TextProximityIndexedSourceResolver); ok {
		return indexed.ResolveSQLTextProximitySource(name, key, field, query, maxGap)
	}
	return nil, false, nil
}

// ResolveSQLTextProximityUnionSource exposes one source-ordered, deduplicated
// positional union to the SQL planner. The complete OR predicate is still
// evaluated by hatSql after candidate resolution.
func (adapter SQLResolverAdapter) ResolveSQLTextProximityUnionSource(name, key, field string, queries []hatSql.SQLTextProximityQuery) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			rows, available := source.lookupTextUnion(field, queries)
			if !available {
				return nil, false, nil
			}
			return sqlRows(rows), true, nil
		}
	}
	if indexed, ok := adapter.Base.(hatSql.TextProximityUnionIndexedSourceResolver); ok {
		return indexed.ResolveSQLTextProximityUnionSource(name, key, field, queries)
	}
	return nil, false, nil
}
