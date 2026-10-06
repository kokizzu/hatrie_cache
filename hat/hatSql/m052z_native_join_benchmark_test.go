package hatSql

import (
	"context"
	"testing"
)

var m052zNativeJoinSink SQLQueryResult

func BenchmarkM052ZAutomaticInnerJoin(b *testing.B) {
	rows := m052zJoinRows()
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return rows.left, nil
		}
		return rows.right, nil
	})
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key WHERE r.value >= 0 SELECT l.id, r.value"
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		m052zNativeJoinSink = result
	}
}

func BenchmarkM052ZFallbackInnerJoin(b *testing.B) {
	rows := m052zJoinRows()
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return rows.left, nil
		}
		return rows.right, nil
	})
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key WHERE r.value >= 0 SELECT l.id, r.value"
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
		if err != nil {
			b.Fatal(err)
		}
		m052zNativeJoinSink = result
	}
}

type m052zJoinInput struct {
	left  []SQLRow
	right []SQLRow
}

func m052zJoinRows() m052zJoinInput {
	rows := m052zJoinInput{
		left:  make([]SQLRow, 256),
		right: make([]SQLRow, 256),
	}
	for index := range rows.left {
		rows.left[index] = SQLRow{"id": int64(index), "key": int64(index % 64)}
	}
	for index := range rows.right {
		rows.right[index] = SQLRow{"key": int64(index % 64), "value": int64(index)}
	}
	return rows
}
