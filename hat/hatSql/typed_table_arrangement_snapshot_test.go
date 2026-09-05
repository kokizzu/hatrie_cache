package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementSnapshotReportsReuseAndFreshness(t *testing.T) {
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
	definition := hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
	first, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	count, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	defer count.Release()
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Apply(changes); err != nil {
		t.Fatal(err)
	}

	snapshot := arrangements.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(snapshot))
	}
	var sharedIndex, staleIndex = -1, -1
	for index, info := range snapshot {
		if reflect.DeepEqual(info.Definition, definition) {
			sharedIndex = index
			if info.TableName != "scores" || info.References != 2 || !info.Shared || info.Checkpoint != 2 || info.SourceSequence != 2 || info.Stale {
				t.Fatalf("shared snapshot = %#v", info)
			}
		}
		if len(info.Definition.GroupBy) == 1 && info.Definition.GroupBy[0] == "team" && info.Definition.SumField == "" {
			staleIndex = index
			if info.References != 1 || info.Shared || info.Checkpoint != 0 || info.SourceSequence != 2 || !info.Stale {
				t.Fatalf("stale snapshot = %#v", info)
			}
		}
	}
	if sharedIndex < 0 || staleIndex < 0 {
		t.Fatalf("snapshot = %#v, missing aggregate definitions", snapshot)
	}
	if got := arrangements.Snapshot(); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("repeated snapshot changed: first=%#v second=%#v", snapshot, got)
	}

	snapshot[sharedIndex].Definition.GroupBy[0] = "mutated"
	if got := arrangements.Snapshot()[sharedIndex].Definition.GroupBy[0]; got != "team" {
		t.Fatalf("snapshot leaked mutable definition, got %q", got)
	}

	if !first.Release() {
		t.Fatal("first Release() failed")
	}
	remaining := arrangements.Snapshot()
	for _, info := range remaining {
		if reflect.DeepEqual(info.Definition, definition) && info.References != 1 {
			t.Fatalf("remaining shared references = %d, want 1", info.References)
		}
	}
}

func TestTypedTableJoinArrangementSnapshotReportsReuseAndFreshness(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "people",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("lin", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	definition := hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
	first, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	change, err := left.Upsert("lin", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}

	snapshot := arrangements.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(snapshot))
	}
	info := snapshot[0]
	if info.LeftTableName != "scores" || info.RightTableName != "people" || !reflect.DeepEqual(info.Definition, definition) {
		t.Fatalf("join identity snapshot = %#v", info)
	}
	if info.References != 2 || !info.Shared || info.LeftCheckpoint != 2 || info.LeftSourceSequence != 2 || info.RightCheckpoint != 1 || info.RightSourceSequence != 1 || info.Stale {
		t.Fatalf("join snapshot = %#v", info)
	}

	if !first.Release() {
		t.Fatal("first Release() failed")
	}
	if got := arrangements.Snapshot()[0].References; got != 1 {
		t.Fatalf("remaining join references = %d, want 1", got)
	}
	if !second.Release() {
		t.Fatal("second Release() failed")
	}
	if got := arrangements.Snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after final release = %#v, want empty", got)
	}
}

func TestTypedTableArrangementSnapshotNilRegistryIsSafe(t *testing.T) {
	var aggregates *hatSql.TypedTableAggregateArrangements
	if aggregates.Snapshot() != nil {
		t.Fatal("nil aggregate registry snapshot is not nil")
	}
	var joins *hatSql.TypedTableJoinArrangements
	if joins.Snapshot() != nil {
		t.Fatal("nil join registry snapshot is not nil")
	}
}

func BenchmarkTypedTableAggregateArrangementSnapshot(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		b.Fatal(err)
	}
	defer lease.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = arrangements.Snapshot()
	}
}

func BenchmarkTypedTableAggregateArrangementActive(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		b.Fatal(err)
	}
	defer lease.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = arrangements.Active()
	}
}

func BenchmarkTypedTableJoinArrangementSnapshot(b *testing.B) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "people",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		b.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		b.Fatal(err)
	}
	defer lease.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = arrangements.Snapshot()
	}
}

func BenchmarkTypedTableJoinArrangementActive(b *testing.B) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "people",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		b.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		b.Fatal(err)
	}
	defer lease.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = arrangements.Active()
	}
}
