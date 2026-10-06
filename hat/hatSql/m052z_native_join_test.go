package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestM052ZCompiledSQLAutomaticNativeInnerJoinMatchesFallback(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		switch key {
		case "left":
			return []SQLRow{
				{"id": int64(1), "key": "a"},
				{"id": int64(2), "key": "a"},
				{"id": int64(3), "key": nil},
				{"id": int64(4), "key": "missing"},
			}, nil
		case "right":
			return []SQLRow{
				{"key": "a", "name": "Ada", "active": int64(1)},
				{"key": "a", "name": "Bea", "active": int64(0)},
				{"key": nil, "name": "Null", "active": int64(1)},
				{"key": "missing", "name": "Mia", "active": int64(0)},
			}, nil
		default:
			return nil, nil
		}
	})
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key WHERE r.active = 1 SELECT l.id, r.name"
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic inner join: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("fallback inner join: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	want := []SQLRow{{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Ada"}}
	if !reflect.DeepEqual(auto.Rows, want) {
		t.Fatalf("automatic rows = %#v, want %#v", auto.Rows, want)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestM052ZCompiledSQLAutomaticInnerJoinKeepsUnsupportedOuterJoinOnFallback(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return []SQLRow{{"id": int64(1), "key": "missing"}}, nil
		}
		return []SQLRow{{"key": "present", "name": "Ada"}}, nil
	})
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.name", resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("outer join: %v", err)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("outer join unexpectedly used native dataflow: %#v", result.Plan)
	}
	want := []SQLRow{{"id": int64(1), "name": nil}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("outer join rows = %#v, want %#v", result.Rows, want)
	}
}

func TestM052ZCompiledSQLAutomaticInnerJoinLimitZeroReturnsNoRows(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return []SQLRow{{"id": int64(1), "key": "a"}}, nil
		}
		return []SQLRow{{"key": "a", "name": "Ada"}}, nil
	})
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.name LIMIT 0", resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("limit-zero join: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("limit-zero rows = %#v, want empty", result.Rows)
	}
	if !m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("limit-zero plan = %#v, want NATIVE DATAFLOW", result.Plan)
	}
}
