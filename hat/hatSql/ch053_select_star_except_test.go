package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLSelectStarExcept(t *testing.T) {
	query := `FROM VALUES (1, 'alice', true), (2, 'bob', false) AS src(id, name, active) SELECT * EXCEPT (name)`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{
		{"id": int64(1), "active": true},
		{"id": int64(2), "active": false},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}

	var streamed []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed rows = %#v, want %#v", streamed, want)
	}

	all, err := ExecuteSQLQueryContext(context.Background(), `FROM VALUES (1, 'alice', true) AS src(id, name, active) SELECT *`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SELECT * error = %v", err)
	}
	if wantAll := []SQLRow{{"id": int64(1), "name": "alice", "active": true}}; !reflect.DeepEqual(all.Rows, wantAll) {
		t.Fatalf("SELECT * rows = %#v, want %#v", all.Rows, wantAll)
	}
	cache := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(3), "name": "carol", "active": true}}, nil
	})
	fast, err := ExecuteSQLQueryContext(context.Background(), `FROM CACHE('events') SELECT * EXCEPT (name)`, cache, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("CACHE SELECT * EXCEPT error = %v", err)
	}
	if wantFast := []SQLRow{{"id": int64(3), "active": true}}; !reflect.DeepEqual(fast.Rows, wantFast) {
		t.Fatalf("CACHE SELECT * EXCEPT rows = %#v, want %#v", fast.Rows, wantFast)
	}

	for _, invalid := range []string{
		`FROM VALUES (1) AS src(id) SELECT * EXCEPT ()`,
		`FROM VALUES (1) AS src(id) SELECT * EXCEPT (id, id)`,
		`FROM VALUES (1) AS src(id) SELECT * EXCEPT (id, )`,
	} {
		if _, err := ExecuteSQLQueryContext(context.Background(), invalid, nil, SQLQueryOptions{}); err == nil {
			t.Errorf("query %q succeeded, want syntax error", invalid)
		}
	}
}

func BenchmarkSQLSelectStarExcept(b *testing.B) {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"name":   "benchmark",
			"active": index%2 == 0,
			"score":  int64(index * 3),
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "explicit", query: "FROM CACHE('events') SELECT id, active, score"},
		{name: "except", query: "FROM CACHE('events') SELECT * EXCEPT (name)"},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				if _, err := ExecuteSQLQuery(test.query, resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
