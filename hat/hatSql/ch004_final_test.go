package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCH004FinalReplacingReadIsOptInAndStable(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "version": uint64(1), "value": "old"},
		{"id": "b", "version": uint64(1), "value": "b"},
		{"id": "a", "version": uint64(3), "value": "new"},
		{"id": "a", "version": uint64(2), "value": "stale"},
	}
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return rows, nil
	})

	legacy, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id, event.value ORDER BY event.id",
		resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("legacy query: %v", err)
	}
	if len(legacy.Rows) != len(rows) {
		t.Fatalf("legacy rows = %d, want %d", len(legacy.Rows), len(rows))
	}

	final, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id, event.value ORDER BY event.id",
		resolver, nil, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004ReplacingFinalOptions)})
	if err != nil {
		t.Fatalf("FINAL query: %v", err)
	}
	want := []SQLRow{
		{"id": "a", "value": "new"},
		{"id": "b", "value": "b"},
	}
	if !reflect.DeepEqual(final.Rows, want) {
		t.Fatalf("FINAL rows = %#v, want %#v", final.Rows, want)
	}

	_, err = ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id",
		resolver, nil, SQLQueryOptions{})
	parsed, parseErr := parseSQLQuery("FROM CACHE('events') AS event FINAL SELECT event.id")
	if parseErr != nil || parsed.from == nil || !parsed.from.final {
		t.Fatalf("parsed FINAL source = %#v/%v, want final source", parsed, parseErr)
	}
	if !errors.Is(err, ErrSQLFinalOptionsRequired) {
		t.Fatalf("missing FINAL options error = %v, want %v", err, ErrSQLFinalOptionsRequired)
	}
}

func TestCH004FinalCollapsingReadWorksThroughRowStreaming(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "sign": 1},
		{"id": "a", "sign": -1},
		{"id": "b", "sign": 1},
	}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	var got []SQLRow
	err := ExecuteSQLQueryRows(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id ORDER BY event.id",
		resolver, nil, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004CollapsingFinalOptions)},
		func(_ []string, row SQLRow) error {
			got = append(got, row)
			return nil
		})
	if err != nil {
		t.Fatalf("streaming FINAL query: %v", err)
	}
	want := []SQLRow{{"id": "b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("streaming FINAL rows = %#v, want %#v", got, want)
	}
}

func TestCH004FinalValidatesContractsAndAliasForms(t *testing.T) {
	rows := []SQLRow{{"id": "a", "version": uint64(1)}}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	contracts := []struct {
		name    string
		options SQLFinalOptions
	}{
		{name: "missing key", options: SQLFinalOptions{Mode: SQLFinalReplacing, Version: func(SQLRow) (uint64, error) { return 1, nil }}},
		{name: "replacing sign", options: SQLFinalOptions{Mode: SQLFinalReplacing, Key: func(SQLRow) string { return "a" }, Version: func(SQLRow) (uint64, error) { return 1, nil }, Sign: func(SQLRow) (int, error) { return 1, nil }}},
		{name: "collapsing sign", options: SQLFinalOptions{Mode: SQLFinalCollapsing, Key: func(SQLRow) string { return "a" }}},
		{name: "collapsing version", options: SQLFinalOptions{Mode: SQLFinalCollapsing, Key: func(SQLRow) string { return "a" }, Sign: func(SQLRow) (int, error) { return 1, nil }, Version: func(SQLRow) (uint64, error) { return 1, nil }}},
	}
	for _, test := range contracts {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExecuteSQLQueryContext(context.Background(),
				"FROM CACHE('events') FINAL AS event SELECT event.id",
				resolver, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(func(string, string) (SQLFinalOptions, bool, error) {
					return test.options, true, nil
				})})
			if !errors.Is(err, ErrSQLFinalOptionsInvalid) {
				t.Fatalf("invalid FINAL contract error = %v, want %v", err, ErrSQLFinalOptionsInvalid)
			}
		})
	}

	_, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') FINAL AS event SELECT event.id",
		resolver, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(func(string, string) (SQLFinalOptions, bool, error) {
			return SQLFinalOptions{}, false, nil
		})})
	if !errors.Is(err, ErrSQLFinalOptionsRequired) {
		t.Fatalf("unconfigured FINAL source error = %v, want %v", err, ErrSQLFinalOptionsRequired)
	}
}

func TestCH004FinalResolverErrorIsReturned(t *testing.T) {
	wantErr := errors.New("final resolver failed")
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		return []Row{{"id": "a"}}, nil
	})
	_, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id",
		resolver, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(func(string, string) (SQLFinalOptions, bool, error) {
			return SQLFinalOptions{}, false, wantErr
		})})
	if !errors.Is(err, wantErr) {
		t.Fatalf("FINAL resolver error = %v, want %v", err, wantErr)
	}
}

func ch004ReplacingFinalOptions(kind, key string) (SQLFinalOptions, bool, error) {
	if kind != "CACHE" || key != "events" {
		return SQLFinalOptions{}, false, nil
	}
	return SQLFinalOptions{
		Mode:    SQLFinalReplacing,
		Key:     func(row SQLRow) string { return row["id"].(string) },
		Version: func(row SQLRow) (uint64, error) { return row["version"].(uint64), nil },
	}, true, nil
}

func ch004CollapsingFinalOptions(kind, key string) (SQLFinalOptions, bool, error) {
	if kind != "CACHE" || key != "events" {
		return SQLFinalOptions{}, false, nil
	}
	return SQLFinalOptions{
		Mode: SQLFinalCollapsing,
		Key:  func(row SQLRow) string { return row["id"].(string) },
		Sign: func(row SQLRow) (int, error) { return row["sign"].(int), nil },
	}, true, nil
}

func ch004FinalSourceOptionsResolver(resolve SQLFinalSourceOptionsFunc) *SQLFinalSourceOptionsResolver {
	return &SQLFinalSourceOptionsResolver{Resolve: resolve}
}
