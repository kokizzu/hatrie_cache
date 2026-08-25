package hatSql

import (
	"context"
	"testing"
)

func TestCanonicalSnapshotIsStableAndColumnOrdered(t *testing.T) {
	result := QueryResult{
		Columns: []string{"id", "payload"},
		Rows:    []Row{{"payload": map[string]interface{}{"b": int64(2), "a": int64(1)}, "id": int64(7)}},
	}
	first, err := CanonicalSnapshot(result)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalSnapshot(result)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("snapshots differ:\n%s\n%s", first, second)
	}
	want := "{\"columns\":[\"id\",\"payload\"],\"rows\":[[7,{\"a\":1,\"b\":2}]]}\n"
	if string(first) != want {
		t.Fatalf("snapshot = %s, want %s", first, want)
	}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return []Row{{"id": int64(7)}}, nil })
	if _, err := SnapshotQuery(context.Background(), "FROM CACHE('people') SELECT id", resolver, nil, QueryOptions{}); err != nil {
		t.Fatalf("SnapshotQuery() error = %v", err)
	}
}
