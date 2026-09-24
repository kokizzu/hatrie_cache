package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type chu14SpillRuntimeFilterResolver struct {
	left  []SQLRow
	right []SQLRow
}

func (resolver chu14SpillRuntimeFilterResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	if name != "CACHE" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	switch key {
	case "left":
		return resolver.left, nil
	case "right":
		return resolver.right, nil
	default:
		return nil, fmt.Errorf("unexpected cache key %q", key)
	}
}

func (resolver chu14SpillRuntimeFilterResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(SQLRow) error) error {
	rows, err := resolver.ResolveSQLSource(name, key)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCHU14SpillRuntimeJoinFilter(b *testing.B) {
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	makeRows := func(count, idOffset int) []SQLRow {
		rows := make([]SQLRow, 0, count)
		for index := 0; index < count; index++ {
			rows = append(rows, SQLRow{"id": idOffset + index, "k": fmt.Sprintf("key-%04d", index)})
		}
		return rows
	}
	datasets := []struct {
		name  string
		left  []SQLRow
		right []SQLRow
	}{
		{name: "left-heavy", left: makeRows(4096, 0), right: makeRows(32, 10000)},
		{name: "right-heavy", left: makeRows(32, 0), right: makeRows(4096, 10000)},
	}
	filters := []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "no-bloom"},
		{name: "partition-bloom", options: SQLQueryOptions{SpillBloom: true}},
	}
	for _, dataset := range datasets {
		resolver := chu14SpillRuntimeFilterResolver{left: dataset.left, right: dataset.right}
		for _, filter := range filters {
			b.Run(dataset.name+"/"+filter.name, func(b *testing.B) {
				options := filter.options
				options.JoinOverflowPolicy = SQLJoinOverflowSpill
				options.MaxJoinBytes = 256
				options.SpillDirectory = b.TempDir()
				options.MaxSpillBytes = 64 << 20
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != 32 {
						b.Fatalf("rows = %d, want 32", len(result.Rows))
					}
				}
			})
		}
	}
}
