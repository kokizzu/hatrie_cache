package hatSql_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementCheckpointIsDeterministic(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "deterministic_scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "name", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range []struct {
		team   string
		points int64
		name   string
	}{
		{team: "red", points: 10, name: "zeta"},
		{team: "red", points: 2, name: "alpha"},
		{team: "red", points: 7, name: "mu"},
		{team: "blue", points: 4, name: "theta"},
		{team: "blue", points: 1, name: "beta"},
		{team: "blue", points: 9, name: "eta"},
	} {
		if _, err := table.Upsert("row-"+string(rune('a'+index)), []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.points),
			hatSql.TypedString(row.name),
		}); err != nil {
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
		DistinctField: "name",
	})
	if err != nil {
		t.Fatal(err)
	}
	for {
		hydration, err := arrangement.Hydrate(2)
		if err != nil {
			t.Fatal(err)
		}
		if hydration.Complete {
			break
		}
	}

	first, err := json.Marshal(mustM055Checkpoint(t, arrangement))
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 5; index++ {
		current, err := json.Marshal(mustM055Checkpoint(t, arrangement))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(current, first) {
			t.Fatalf("checkpoint JSON changed on capture %d", index+1)
		}
	}
}

func mustM055Checkpoint(t *testing.T, arrangement *hatSql.TypedTableAggregateArrangement) hatSql.TypedTableAggregateArrangementCheckpoint {
	t.Helper()
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
