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
