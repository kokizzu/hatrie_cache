package hatSql

import "testing"

type tt024CrossFieldUnionResolver struct {
	rows        []Row
	indexedRows []Row
	calls       int
	queries     []SQLTextProximityFieldQuery
}

type tt024CrossFieldLegacyResolver struct {
	rows  []Row
	calls int
}

func (resolver *tt024CrossFieldLegacyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *tt024CrossFieldLegacyResolver) ResolveSQLTextProximityUnionSource(string, string, string, []SQLTextProximityQuery) ([]Row, bool, error) {
	resolver.calls++
	return resolver.rows, true, nil
}

func (resolver *tt024CrossFieldUnionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *tt024CrossFieldUnionResolver) ResolveSQLTextProximityMultiFieldUnionSource(_ string, _ string, queries []SQLTextProximityFieldQuery) ([]Row, bool, error) {
	resolver.calls++
	resolver.queries = append(resolver.queries[:0], queries...)
	return resolver.indexedRows, true, nil
}

func TestSQLContainsPhraseMixedORUsesMultiFieldUnionIndex(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "kind": "a", "title": "quick brown fox", "body": "unrelated"},
		{"id": int64(2), "kind": "b", "title": "unrelated", "body": "lazy red fox"},
		{"id": int64(3), "kind": "a", "title": "quick brown fox", "body": "lazy red fox"},
		{"id": int64(4), "kind": "other", "title": "unrelated", "body": "unrelated"},
	}
	resolver := &tt024CrossFieldUnionResolver{
		rows:        rows,
		indexedRows: []Row{rows[0], rows[1], rows[2]},
	}
	result, err := ExecuteSQLQuery(`
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.title, 'quick brown') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) AND doc.kind = 'b')
SELECT doc.id
ORDER BY doc.id`, resolver)
	if err != nil {
		t.Fatalf("cross-field mixed OR query error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"id"}, Rows: []SQLRow{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}}
	if got := result; !sqlQueryResultsEqual(got, want) {
		t.Fatalf("cross-field mixed OR result = %#v, want %#v", got, want)
	}
	if resolver.calls != 1 {
		t.Fatalf("multi-field union resolver calls = %d, want 1", resolver.calls)
	}
	wantQueries := []SQLTextProximityFieldQuery{
		{Field: "title", Query: SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: SQLTextProximityQuery{Query: "lazy fox", MaxGap: 1, Proximity: true}},
	}
	if len(resolver.queries) != len(wantQueries) {
		t.Fatalf("multi-field union queries = %#v, want %#v", resolver.queries, wantQueries)
	}
	for index := range wantQueries {
		if resolver.queries[index] != wantQueries[index] {
			t.Fatalf("multi-field union query[%d] = %#v, want %#v", index, resolver.queries[index], wantQueries[index])
		}
	}
}

func TestSQLContainsPhraseMixedORCrossFieldLegacyResolverFallsBack(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "kind": "a", "title": "quick brown fox", "body": "unrelated"},
		{"id": int64(2), "kind": "b", "title": "unrelated", "body": "lazy red fox"},
		{"id": int64(3), "kind": "a", "title": "quick brown fox", "body": "lazy red fox"},
	}
	resolver := &tt024CrossFieldLegacyResolver{rows: rows}
	result, err := ExecuteSQLQuery(`
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.title, 'quick brown') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) AND doc.kind = 'b')
SELECT doc.id
ORDER BY doc.id`, resolver)
	if err != nil {
		t.Fatalf("legacy cross-field mixed OR query error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("legacy cross-field mixed OR rows = %d, want 3", len(result.Rows))
	}
	if resolver.calls != 0 {
		t.Fatalf("legacy single-field union resolver calls = %d, want 0", resolver.calls)
	}
}

func sqlQueryResultsEqual(left, right SQLQueryResult) bool {
	if len(left.Columns) != len(right.Columns) || len(left.Rows) != len(right.Rows) {
		return false
	}
	for index := range left.Columns {
		if left.Columns[index] != right.Columns[index] {
			return false
		}
	}
	for rowIndex := range left.Rows {
		if len(left.Rows[rowIndex]) != len(right.Rows[rowIndex]) {
			return false
		}
		for columnIndex := range left.Rows[rowIndex] {
			if left.Rows[rowIndex][columnIndex] != right.Rows[rowIndex][columnIndex] {
				return false
			}
		}
	}
	return true
}
