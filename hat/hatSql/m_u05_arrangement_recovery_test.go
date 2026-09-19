package hatSql_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay(t *testing.T) {
	table, changes, definition := mU05AggregateTable(t)
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.TypedTableAggregateArrangementCheckpoint
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Checkpoint != checkpoint.Checkpoint || len(decoded.Groups) != len(checkpoint.Groups) {
		t.Fatalf("checkpoint round trip = %#v, want %#v", decoded, checkpoint)
	}

	recoveredArrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{decoded})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Checkpoint() != checkpoint.Checkpoint {
		t.Fatalf("recovered arrangements = %d checkpoint = %d", len(recovered), recovered[0].Checkpoint())
	}
	if got, want := recovered[0].Rows(), arrangement.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered rows = %#v, want %#v", got, want)
	}

	update, err := table.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(9), hatSql.TypedString("ada")})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered[0].Apply([]hatSql.TypedTableChange{update}); err != nil {
		t.Fatal(err)
	}
	fresh, err := hatSql.NewTypedTableAggregate(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Apply(append(changes, update)); err != nil {
		t.Fatal(err)
	}
	if got, want := recovered[0].Rows(), fresh.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered continuation = %#v, fresh replay = %#v", got, want)
	}
	recovered[0].Release()
	arrangement.Release()
}

func TestTypedTableArrangementCheckpointRestoreFencesVersionsAndIsAtomic(t *testing.T) {
	table, changes, definition := mU05AggregateTable(t)
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	badVersion := checkpoint
	badVersion.SourceSequence++
	recoveredArrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{checkpoint, badVersion}); !errors.Is(err, hatSql.ErrTypedTableArrangementCheckpointDuplicate) {
		t.Fatalf("duplicate restore error = %v", err)
	}
	if recoveredArrangements.Active() != 0 {
		t.Fatalf("failed restore left %d active arrangements", recoveredArrangements.Active())
	}
	badVersion = checkpoint
	badVersion.SourceSequence++
	if _, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{badVersion}); !errors.Is(err, hatSql.ErrTypedTableArrangementSourceVersionMismatch) {
		t.Fatalf("source version error = %v", err)
	}
	if recoveredArrangements.Active() != 0 {
		t.Fatalf("version failure left %d active arrangements", recoveredArrangements.Active())
	}
	arrangement.Release()
}

func TestTypedTableJoinArrangementCheckpointRestoresBothInputs(t *testing.T) {
	left, leftChanges := mU05JoinTable(t, "left", []string{"left-red", "left-blue"})
	right, rightChanges := mU05JoinTable(t, "right", []string{"right-red"})
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	definition := hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.ApplyLeft(leftChanges); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.ApplyRight(rightChanges); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.TypedTableJoinArrangementCheckpoint
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	recoveredArrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := recoveredArrangements.RestoreCheckpoints([]hatSql.TypedTableJoinArrangementCheckpoint{decoded})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recovered[0].Rows(), arrangement.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered join rows = %#v, want %#v", got, want)
	}

	leftChange, err := left.Upsert("left-red", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedString("L2")})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered[0].ApplyLeft([]hatSql.TypedTableChange{leftChange}); err != nil {
		t.Fatal(err)
	}
	fresh, err := hatSql.NewTypedTableJoin(left, right, definition)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recovered[0].Rows(), fresh.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered join continuation = %#v, fresh = %#v", got, want)
	}
	recovered[0].Release()
	arrangement.Release()
}

func mU05AggregateTable(t *testing.T) (*hatSql.TypedTable, []hatSql.TypedTableChange, hatSql.TypedTableAggregateDefinition) {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "name", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	inputs := []struct {
		key    string
		team   string
		points int64
		name   string
	}{
		{key: "ada", team: "red", points: 1, name: "ada"},
		{key: "lin", team: "blue", points: 2, name: "lin"},
		{key: "max", team: "red", points: 3, name: "max"},
	}
	changes := make([]hatSql.TypedTableChange, 0, len(inputs))
	for _, input := range inputs {
		change, err := table.Upsert(input.key, []hatSql.TypedTableValue{hatSql.TypedString(input.team), hatSql.TypedInt64(input.points), hatSql.TypedString(input.name)})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, change)
	}
	return table, changes, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points", MinField: "points", MaxField: "points", DistinctField: "name"}
}

func mU05JoinTable(t *testing.T, name string, keys []string) (*hatSql.TypedTable, []hatSql.TypedTableChange) {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "label", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 0, len(keys))
	for index, key := range keys {
		team := "red"
		if index > 0 {
			team = "blue"
		}
		change, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedString(key)})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, change)
	}
	return table, changes
}
