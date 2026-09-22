package hatSql_test

import (
	"encoding/json"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM243ArrangementMemoryMetricsSplitEstimatedBytes(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "m243_metrics",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key    string
		team   string
		points int64
	}{
		{key: "ada", team: "red", points: 4},
		{key: "lin", team: "red", points: 6},
		{key: "max", team: "blue", points: 8},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.points)}); err != nil {
			t.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "points",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	stats, err := arrangement.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.EstimatedKeyBytes == 0 || stats.EstimatedValueBytes == 0 || stats.EstimatedTraceBytes == 0 {
		t.Fatalf("split arrangement bytes = %#v, want all components nonzero", stats)
	}
	if got := stats.EstimatedKeyBytes + stats.EstimatedValueBytes + stats.EstimatedTraceBytes; got != stats.EstimatedBytes {
		t.Fatalf("split bytes = %d, legacy total = %d", got, stats.EstimatedBytes)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"estimated_key_bytes", "estimated_value_bytes", "estimated_trace_bytes"} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("JSON stats %q missing %q", encoded, field)
		}
	}
}
