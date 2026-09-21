package hatSql_test

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM208FoldQuerySubscriptionDeltasPreservesNetAndOrder(t *testing.T) {
	ada := hatSql.Row{"id": int64(1), "name": "Ada"}
	lin := hatSql.Row{"id": int64(2), "name": "Lin"}
	input := []hatSql.QuerySubscriptionDelta{
		{Row: ada, Diff: 1},
		{Row: lin, Diff: 2},
		{Row: ada, Diff: 1},
		{Row: lin, Diff: -1},
		{Row: ada, Diff: -1},
		{Row: lin, Diff: -1},
		{Row: hatSql.Row{"id": int64(3)}, Diff: 0},
	}
	got, err := hatSql.FoldQuerySubscriptionDeltas(input)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.QuerySubscriptionDelta{
		{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("folded deltas = %#v, want %#v", got, want)
	}
	got[0].Row["name"] = "consumer-mutated"
	if ada["name"] != "Ada" {
		t.Fatal("folded row aliases input row")
	}
}

func TestM208FoldQuerySubscriptionDeltaBatchPreservesMetadata(t *testing.T) {
	input := hatSql.QuerySubscriptionDeltaBatch{
		ID:       9,
		Revision: 10,
		Frontier: 11,
		Columns:  []string{"id", "name"},
		Reset:    true,
		Complete: true,
		Deltas: []hatSql.QuerySubscriptionDelta{
			{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: 1},
			{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: -1},
		},
	}
	got, err := hatSql.FoldQuerySubscriptionDeltaBatch(input)
	if err != nil {
		t.Fatal(err)
	}
	want := input
	want.Deltas = nil
	want.Columns = append([]string(nil), input.Columns...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("folded batch = %#v, want %#v", got, want)
	}
	got.Columns[0] = "changed"
	if input.Columns[0] != "id" {
		t.Fatal("folded batch aliases input columns")
	}
}

func TestM208FoldQuerySubscriptionDeltasRejectsOverflow(t *testing.T) {
	_, err := hatSql.FoldQuerySubscriptionDeltas([]hatSql.QuerySubscriptionDelta{
		{Row: hatSql.Row{"id": int64(1)}, Diff: math.MaxInt64},
		{Row: hatSql.Row{"id": int64(1)}, Diff: 1},
	})
	if !errors.Is(err, hatSql.ErrQuerySubscriptionDeltaOverflow) {
		t.Fatalf("overflow error = %v, want %v", err, hatSql.ErrQuerySubscriptionDeltaOverflow)
	}
}

func TestM208FoldQuerySubscriptionDeltasHandlesNaNKeys(t *testing.T) {
	got, err := hatSql.FoldQuerySubscriptionDeltas([]hatSql.QuerySubscriptionDelta{
		{Row: hatSql.Row{"value": math.NaN()}, Diff: 1},
		{Row: hatSql.Row{"value": math.NaN()}, Diff: -1},
	})
	if err != nil || got != nil {
		t.Fatalf("NaN cancellation = %#v, %v; want nil, nil", got, err)
	}
}

func TestM208AdaptersFoldRepeatedIdenticalUpdates(t *testing.T) {
	batch := hatSql.QuerySubscriptionDeltaBatch{
		Deltas: []hatSql.QuerySubscriptionDelta{
			{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: 1},
			{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: 1},
			{Row: hatSql.Row{"id": int64(1), "name": "Ada"}, Diff: -1},
		},
	}

	upsert, err := hatSql.NewUpsertChangefeed(hatSql.UpsertChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	upsertEvents, err := upsert.Apply(batch)
	if err != nil {
		t.Fatalf("upsert Apply() error = %v", err)
	}
	if len(upsertEvents) != 1 || upsertEvents[0].Current["name"] != "Ada" {
		t.Fatalf("upsert events = %#v, want one Ada upsert", upsertEvents)
	}

	debezium, err := hatSql.NewDebeziumChangefeed(hatSql.DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	debeziumEvents, err := debezium.Apply(batch)
	if err != nil {
		t.Fatalf("debezium Apply() error = %v", err)
	}
	if len(debeziumEvents) != 1 || debeziumEvents[0].Payload.After["name"] != "Ada" {
		t.Fatalf("debezium events = %#v, want one Ada read", debeziumEvents)
	}
}
