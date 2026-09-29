package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementsExposeMemoryAndFreshnessStats(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "scores",
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
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.points)}); err != nil {
			t.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	definition := hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points", DistinctField: "points", DictionaryEncodeGroups: true}
	first, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Apply(changes); err != nil {
		t.Fatal(err)
	}
	if rows := first.Rows(); len(rows) != 1 {
		t.Fatalf("aggregate rows = %#v", rows)
	}

	stats := arrangements.Stats()
	if stats.ActiveDefinitions != 1 || stats.ActiveLeases != 2 || stats.SourceSequence != 2 || len(stats.Arrangements) != 1 {
		t.Fatalf("arrangement stats = %#v", stats)
	}
	arrangementStats := stats.Arrangements[0]
	if arrangementStats.References != 2 || arrangementStats.Checkpoint != 2 || arrangementStats.SourceSequence != 2 || arrangementStats.CompactedThrough != 0 || arrangementStats.Groups != 1 || arrangementStats.DistinctValues != 2 || arrangementStats.CompactionCount != 1 || arrangementStats.EstimatedBytes == 0 {
		t.Fatalf("arrangement detail stats = %#v", arrangementStats)
	}
	if arrangementStats.KeyBytes == 0 || arrangementStats.ValueBytes == 0 || arrangementStats.TraceBytes == 0 {
		t.Fatalf("arrangement memory breakdown = %#v", arrangementStats)
	}
	if arrangementStats.EstimatedBytes != arrangementStats.KeyBytes+arrangementStats.ValueBytes || arrangementStats.RetainedBytes != arrangementStats.EstimatedBytes+arrangementStats.TraceBytes {
		t.Fatalf("arrangement memory total = %#v", arrangementStats)
	}
	keyBytes := arrangementStats.KeyBytes
	valueBytes := arrangementStats.ValueBytes
	traceBytes := arrangementStats.TraceBytes
	compaction := arrangements.CompactionStats()
	if compaction.LogicalFrontier != 2 || compaction.CompactedThrough != 0 || compaction.CompactionDebt != 2 {
		t.Fatalf("compaction stats = %#v", compaction)
	}
	individual, err := first.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if individual != arrangementStats {
		t.Fatalf("individual stats = %#v, collection = %#v", individual, arrangementStats)
	}
	if err := table.CompactChangesThrough(1); err != nil {
		t.Fatal(err)
	}
	if got, err := first.Stats(); err != nil || got.CompactedThrough != 1 {
		t.Fatalf("stats after changelog compaction = %#v, err = %v", got, err)
	}
	if got, err := first.Stats(); err != nil || got.KeyBytes != keyBytes || got.ValueBytes != valueBytes || got.TraceBytes >= traceBytes || got.EstimatedBytes != got.KeyBytes+got.ValueBytes || got.RetainedBytes != got.EstimatedBytes+got.TraceBytes {
		t.Fatalf("memory breakdown after changelog compaction = %#v, err = %v", got, err)
	}
	if got, err := first.CompactionStats(); err != nil || got.CompactedThrough != 1 || got.CompactionDebt != 1 {
		t.Fatalf("compaction stats after changelog compaction = %#v, err = %v", got, err)
	}
	stats.Arrangements[0].DefinitionKey = "mutated"
	if arrangements.Stats().Arrangements[0].DefinitionKey == "mutated" {
		t.Fatal("Stats() returned mutable internal state")
	}

	first.Release()
	if got := arrangements.Stats(); got.ActiveDefinitions != 1 || got.ActiveLeases != 1 {
		t.Fatalf("stats after first release = %#v", got)
	}
	second.Release()
	if got := arrangements.Stats(); got.ActiveDefinitions != 0 || got.ActiveLeases != 0 || len(got.Arrangements) != 0 {
		t.Fatalf("stats after final release = %#v", got)
	}
	if _, err := first.Stats(); err == nil {
		t.Fatal("released arrangement Stats() succeeded")
	}
}
