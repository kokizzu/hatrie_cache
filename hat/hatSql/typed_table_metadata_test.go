package hatSql_test

import (
	"errors"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableChangeMetadataTracksAppendOnlyHistory(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "metadata_table",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "points", Kind: hatSql.TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := table.ChangeMetadata(); !metadata.AppendOnly {
		t.Fatal("empty table metadata is not append-only")
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}

	change, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := table.ChangeMetadata(); !metadata.AppendOnly {
		t.Fatal("insert changed append-only metadata")
	}
	if err := aggregate.ApplyWithMetadata([]hatSql.TypedTableChange{change}, table.ChangeMetadata()); err != nil {
		t.Fatal(err)
	}

	change, err = table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := table.ChangeMetadata(); metadata.AppendOnly {
		t.Fatal("update left append-only metadata enabled")
	}
	if err := aggregate.ApplyWithMetadata([]hatSql.TypedTableChange{change}, table.ChangeMetadata()); err != nil {
		t.Fatal(err)
	}

	change, err = table.Delete("a")
	if err != nil {
		t.Fatal(err)
	}
	if metadata := table.ChangeMetadata(); metadata.AppendOnly {
		t.Fatal("delete left append-only metadata enabled")
	}
	if err := aggregate.ApplyWithMetadata([]hatSql.TypedTableChange{change}, table.ChangeMetadata()); err != nil {
		t.Fatal(err)
	}
	if rows := aggregate.Rows(); len(rows) != 0 {
		t.Fatalf("aggregate rows after delete = %#v, want empty", rows)
	}
}

func TestTypedTableAggregateMetadataStillValidatesTrustedAppendOnlyBatches(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "metadata_validation_table",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	change := hatSql.TypedTableChange{
		Sequence:  1,
		Operation: "UPDATE",
		Key:       "a",
		Before:    []hatSql.TypedTableValue{hatSql.TypedString("blue")},
		After:     []hatSql.TypedTableValue{hatSql.TypedString("red")},
	}
	if err := aggregate.ApplyWithMetadata([]hatSql.TypedTableChange{change}, hatSql.TypedTableChangeMetadata{AppendOnly: true}); !errors.Is(err, hatSql.ErrTypedTableAggregateMonotoneBefore) {
		t.Fatalf("trusted append-only error = %v, want monotone validation error", err)
	}
	if aggregate.Checkpoint() != 0 {
		t.Fatalf("checkpoint after rejected metadata batch = %d, want 0", aggregate.Checkpoint())
	}
}

func BenchmarkTypedTableAggregateApplyWithMetadata(b *testing.B) {
	changes := make([]hatSql.TypedTableChange, 4096)
	for index := range changes {
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(index + 1),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%05d", index),
			After:     []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)},
		}
	}
	b.Run("auto", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			table, aggregate := newMetadataBenchmarkAggregate(b)
			if err := aggregate.ApplyAuto(changes); err != nil {
				b.Fatal(err)
			}
			if !table.ChangeMetadata().AppendOnly {
				b.Fatal("benchmark source metadata unexpectedly changed")
			}
		}
	})
	b.Run("metadata", func(b *testing.B) {
		b.ReportAllocs()
		metadata := hatSql.TypedTableChangeMetadata{AppendOnly: true}
		for range b.N {
			_, aggregate := newMetadataBenchmarkAggregate(b)
			if err := aggregate.ApplyWithMetadata(changes, metadata); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func newMetadataBenchmarkAggregate(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTableAggregate) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "metadata_benchmark_table",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "points", Kind: hatSql.TypedTableInt64}},
	})
	if err != nil {
		b.Fatal(err)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	return table, aggregate
}
