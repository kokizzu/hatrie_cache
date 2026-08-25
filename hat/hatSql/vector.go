package hatSql

import (
	"context"
	"fmt"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

// VectorMatch joins a ranked vector result to the SQL row that admitted it.
type VectorMatch struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
	Row   Row     `json:"row"`
}

// SearchVectorHybrid evaluates the SQL filter first, then ranks only vectors
// whose IDs appear in the filtered result.
func SearchVectorHybrid(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions, index *hatDataStructure.VectorIndex, query []float32, limit int, idField string) ([]VectorMatch, error) {
	if index == nil {
		return nil, fmt.Errorf("hatSql: vector index is required")
	}
	idField = strings.TrimSpace(idField)
	if idField == "" {
		return nil, fmt.Errorf("hatSql: vector SQL id field is required")
	}
	result, err := ExecuteQueryParameters(ctx, source, resolver, parameters, options)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]Row, len(result.Rows))
	for _, row := range result.Rows {
		value, exists := row[idField]
		if !exists || value == nil {
			return nil, fmt.Errorf("hatSql: vector SQL row is missing id field %q", idField)
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id == "" {
			return nil, fmt.Errorf("hatSql: vector SQL id field %q is empty", idField)
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
	out := make([]VectorMatch, len(matches))
	for position, match := range matches {
		out[position] = VectorMatch{ID: match.ID, Score: match.Score, Row: rows[match.ID]}
	}
	return out, nil
}
