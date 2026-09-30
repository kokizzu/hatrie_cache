package hatSql

import (
	"reflect"
	"testing"
)

type tt024CrossFieldTextResolver struct {
	rows    []Row
	queries []SQLTextProximityFieldQuery
	calls   int
}

type tt024CrossFieldTextScanResolver struct {
	rows []Row
}

func (resolver *tt024CrossFieldTextScanResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *tt024CrossFieldTextResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *tt024CrossFieldTextResolver) ResolveSQLTextProximityMultiFieldUnionSource(_, _ string, queries []SQLTextProximityFieldQuery) ([]Row, bool, error) {
	resolver.calls++
	resolver.queries = append([]SQLTextProximityFieldQuery(nil), queries...)
	return resolver.rows, true, nil
}

func TestSQLContainsPhraseCrossFieldORUsesMultiFieldIndex(t *testing.T) {
	resolver := &tt024CrossFieldTextResolver{rows: []Row{
		{"id": int64(1), "title": "quick brown release", "body": "unrelated"},
		{"id": int64(2), "title": "unrelated", "body": "lazy red fox"},
		{"id": int64(3), "title": "quick release", "body": "unrelated"},
		{"id": int64(4), "title": "quick brown release", "body": "lazy fox"},
	}}
	result, err := ExecuteSQLQuery(`FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.title, 'quick brown') OR CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) SELECT doc.id ORDER BY doc.id`, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("multi-field index calls = %d, want 1", resolver.calls)
	}
	wantQueries := []SQLTextProximityFieldQuery{
		{Field: "title", Query: SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: SQLTextProximityQuery{Query: "lazy fox", MaxGap: 1, Proximity: true}},
	}
	if !reflect.DeepEqual(resolver.queries, wantQueries) {
		t.Fatalf("multi-field queries = %#v, want %#v", resolver.queries, wantQueries)
	}
	want := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLContainsCrossFieldORFallsBackWithoutMultiFieldIndex(t *testing.T) {
	resolver := &tt024CrossFieldTextScanResolver{rows: []Row{
		{"id": int64(1), "title": "quick brown release", "body": "unrelated"},
		{"id": int64(2), "title": "unrelated", "body": "lazy red fox"},
	}}
	result, err := ExecuteSQLQuery(`FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.title, 'quick brown') OR CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) SELECT doc.id ORDER BY doc.id`, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []Row{{"id": int64(1)}, {"id": int64(2)}}) {
		t.Fatalf("fallback rows = %#v", result.Rows)
	}
}
