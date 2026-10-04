package hatSql_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableSortedArrangementCheckpointRecovery(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_checkpoint")
	for _, row := range []struct {
		key, team string
		score     int64
	}{
		{key: "c", team: "blue", score: 2},
		{key: "a", team: "red", score: 3},
		{key: "b", team: "red", score: 1},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}

	definition := hatSql.TypedTableSortedArrangementDefinition{Field: "team"}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.TypedTableSortedArrangementCheckpoint
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(checkpoint, decoded) {
		t.Fatalf("checkpoint changed across JSON round-trip: %#v != %#v", checkpoint, decoded)
	}

	recoveryTable := newSortedArrangementTable(t, "sorted_checkpoint")
	for _, row := range []struct {
		key, team string
		score     int64
	}{
		{key: "x", team: "zulu", score: 8},
		{key: "y", team: "zulu", score: 9},
		{key: "w", team: "zulu", score: 7},
	} {
		if _, err := recoveryTable.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := hatSql.NewTypedTableSortedArrangementFromCheckpoint(recoveryTable, decoded)
	if err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, restored.Rows(), "c", "a", "b")
	if got, want := restored.Checkpoint(), checkpoint.Checkpoint; got != want {
		t.Fatalf("restored checkpoint = %d, want %d", got, want)
	}

	staleTable := newSortedArrangementTable(t, "sorted_checkpoint")
	if _, err := hatSql.NewTypedTableSortedArrangementFromCheckpoint(staleTable, decoded); !errors.Is(err, hatSql.ErrTypedTableArrangementSourceVersionMismatch) {
		t.Fatalf("stale restore error = %v, want source-version mismatch", err)
	}
}

func TestTypedTableSortedArrangementCheckpointRestoresDictionaryState(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "sorted_checkpoint_dictionary",
		Columns: []hatSql.TypedTableColumn{
			{Name: "region", Kind: hatSql.TypedTableString},
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key, region, team string
		score             int64
	}{
		{key: "north-zulu", region: "north", team: "zulu", score: 2},
		{key: "north-alpha", region: "north", team: "alpha", score: 3},
		{key: "south-beta", region: "south", team: "beta", score: 1},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.region), hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	definition := hatSql.TypedTableSortedArrangementDefinition{OrderBy: []hatSql.TypedTableSortedArrangementOrder{
		{Field: "region", DictionaryEncoded: true},
		{Field: "team", Descending: true, DictionaryEncoded: true},
	}}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.RestoreCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "north-zulu", "north-alpha", "south-beta")

	bad := checkpoint
	bad.Rows = append([]hatSql.TypedTableSortedArrangementCheckpointRow(nil), checkpoint.Rows...)
	bad.Rows = append(bad.Rows, checkpoint.Rows[0])
	if err := arrangement.RestoreCheckpoint(bad); !errors.Is(err, hatSql.ErrTypedTableArrangementCheckpointInvalid) {
		t.Fatalf("duplicate-row restore error = %v, want invalid checkpoint", err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "north-zulu", "north-alpha", "south-beta")

	badType := checkpoint
	badType.Rows = append([]hatSql.TypedTableSortedArrangementCheckpointRow(nil), checkpoint.Rows...)
	badType.Rows[0].Values = append([]hatSql.TypedTableValue(nil), checkpoint.Rows[0].Values...)
	badType.Rows[0].Values[2] = hatSql.TypedString("wrong kind")
	if err := arrangement.RestoreCheckpoint(badType); !errors.Is(err, hatSql.ErrTypedTableArrangementCheckpointInvalid) {
		t.Fatalf("wrong-column-kind restore error = %v, want invalid checkpoint", err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "north-zulu", "north-alpha", "south-beta")

	change, err := table.Upsert("south-omega", []hatSql.TypedTableValue{hatSql.TypedString("south"), hatSql.TypedString("omega"), hatSql.TypedInt64(4)})
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "north-zulu", "north-alpha", "south-omega", "south-beta")
}
