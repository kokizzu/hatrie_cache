package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type ngramSegmentResolver struct {
	batch    hatSql.ColumnarBatch
	segments *hatSql.ColumnarNumericSegments
}

func (resolver ngramSegmentResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return nil, fmt.Errorf("unexpected row-source fallback")
}

func (resolver ngramSegmentResolver) ResolveSQLColumnarSource(string, string, []string) (hatSql.ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver ngramSegmentResolver) BorrowSQLColumnarSourceSegments(string, string, []string) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	return resolver.batch, resolver.segments, true, nil
}

func TestColumnarStringNGramSegmentHasNoFalseNegatives(t *testing.T) {
	segment := hatSql.ColumnarStringNGramBloomSegment{}
	segment.Add("alpha needle omega")
	if !segment.MayContainSubstring("needle") {
		t.Fatal("NGram filter reported a false negative")
	}
	if !segment.MayContainSubstring("ne") {
		t.Fatal("short substring must retain the segment")
	}
}

func TestColumnarNGramLikeFilterSkipsDisjointSegment(t *testing.T) {
	first := hatSql.ColumnarStringNGramBloomSegment{}
	first.Add("alpha")
	first.Add("bravo")
	second := hatSql.ColumnarStringNGramBloomSegment{}
	second.Add("needle")
	second.Add("omega")
	resolver := ngramSegmentResolver{
		batch: hatSql.ColumnarBatch{Columns: map[string][]interface{}{"name": {"alpha", "bravo", "needle", "omega"}}, Rows: 4},
		segments: &hatSql.ColumnarNumericSegments{
			RowsPerSegment:          2,
			StringNGramBloomFilters: map[string][]hatSql.ColumnarStringNGramBloomSegment{"name": {first, second}},
		},
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT name WHERE name LIKE '%needle%'", resolver, nil, hatSql.QueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "needle" {
		t.Fatalf("LIKE result = %#v, error = %v", result, err)
	}
}
