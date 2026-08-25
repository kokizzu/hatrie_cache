package hatriecache

import (
	"context"
	"fmt"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

// SQLVectorMatch joins a ranked vector result to the SQL row that admitted it.
type SQLVectorMatch struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
	Row   SQLRow  `json:"row"`
}

// SearchSQLVectorHybrid evaluates the SQL filter first, then applies exact
// cosine ranking only to vectors whose IDs appear in the filtered result.
func SearchSQLVectorHybrid(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, index *hatDataStructure.VectorIndex, query []float32, limit int, idField string) ([]SQLVectorMatch, error) {
	if index == nil {
		return nil, fmt.Errorf("hatriecache: vector index is required")
	}
	idField = strings.TrimSpace(idField)
	if idField == "" {
		return nil, fmt.Errorf("hatriecache: vector SQL id field is required")
	}
	result, err := ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]SQLRow, len(result.Rows))
	for _, row := range result.Rows {
		value, exists := row[idField]
		if !exists || value == nil {
			return nil, fmt.Errorf("hatriecache: vector SQL row is missing id field %q", idField)
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id == "" {
			return nil, fmt.Errorf("hatriecache: vector SQL id field %q is empty", idField)
		}
		rows[id] = row
	}
	matches, err := index.Search(query, limit, func(id string) bool {
		_, exists := rows[id]
		return exists
	})
	if err != nil {
		return nil, err
	}
	out := make([]SQLVectorMatch, len(matches))
	for position, match := range matches {
		out[position] = SQLVectorMatch{ID: match.ID, Score: match.Score, Row: rows[match.ID]}
	}
	return out, nil
}
