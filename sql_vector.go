package hatriecache

import (
	"context"

	"hatrie_cache/hat/hatDataStructure"
	"hatrie_cache/hat/hatSql"
)

type SQLVectorMatch = hatSql.VectorMatch

// SearchSQLVectorHybrid evaluates the SQL filter first, then ranks only the
// vectors admitted by the filtered result.
func SearchSQLVectorHybrid(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, index *hatDataStructure.VectorIndex, query []float32, limit int, idField string) ([]SQLVectorMatch, error) {
	return hatSql.SearchVectorHybrid(ctx, source, resolver, parameters, options, index, query, limit, idField)
}
