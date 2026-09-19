package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementCheckpointRejectsImpossibleCounts(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "validated_scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("row-1", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(7)}); err != nil {
		t.Fatal(err)
	}
	definition := hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, MinField: "points"}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arrangement.Hydrate(0); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.Groups[0].MinValues[0].Count = checkpoint.Groups[0].Count + 1
	recoveredArrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{checkpoint}); !errors.Is(err, hatSql.ErrTypedTableArrangementCheckpointInvalid) {
		t.Fatalf("invalid count error = %v", err)
	}
	if recoveredArrangements.Active() != 0 {
		t.Fatalf("invalid checkpoint left %d active arrangements", recoveredArrangements.Active())
	}
	arrangement.Release()
}
