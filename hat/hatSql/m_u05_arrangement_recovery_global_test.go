package hatSql_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "global_scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "name", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, row := range map[string][]hatSql.TypedTableValue{
		"a": {hatSql.TypedInt64(4), hatSql.TypedString("ada")},
		"b": {hatSql.TypedInt64(9), hatSql.TypedString("lin")},
	} {
		if _, err := table.Upsert(key, row); err != nil {
			t.Fatal(err)
		}
	}
	definition := hatSql.TypedTableAggregateDefinition{
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "name",
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	for {
		hydration, err := arrangement.Hydrate(1)
		if err != nil {
			t.Fatal(err)
		}
		if hydration.Complete {
			break
		}
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.TypedTableAggregateArrangementCheckpoint
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	recoveredArrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{decoded})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || !reflect.DeepEqual(arrangement.Rows(), recovered[0].Rows()) {
		t.Fatalf("global aggregate rows differ: original=%v recovered=%v", arrangement.Rows(), recovered[0].Rows())
	}
	recovered[0].Release()
	arrangement.Release()
}
