package hatSql_test

import (
	"encoding/json"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM244ArrangementStatsExposeCompactionDebtAtLogicalFrontier(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "m244_debt",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"red", "blue", "green"} {
		if _, err := table.Upsert("row-"+string(rune('a'+index)), []hatSql.TypedTableValue{
			hatSql.TypedString(team),
			hatSql.TypedInt64(int64(index + 1)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
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
	assertM244CompactionDebt(t, arrangement, 3)

	if err := table.CompactChangesThrough(1); err != nil {
		t.Fatal(err)
	}
	assertM244CompactionDebt(t, arrangement, 2)

	if err := table.CompactChangesThrough(3); err != nil {
		t.Fatal(err)
	}
	stats, err := arrangement.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.CompactionDebt != 0 {
		t.Fatalf("fully compacted debt = %d, want 0", stats.CompactionDebt)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "\"compaction_debt\":0") {
		t.Fatalf("JSON stats %q missing compaction debt", encoded)
	}
}

func assertM244CompactionDebt(t *testing.T, arrangement *hatSql.TypedTableAggregateArrangement, want uint64) {
	t.Helper()
	stats, err := arrangement.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SourceSequence != 3 || stats.CompactedThrough+stats.CompactionDebt != stats.SourceSequence || stats.CompactionDebt != want {
		t.Fatalf("frontier stats = %#v, want source 3 and debt %d", stats, want)
	}
}
