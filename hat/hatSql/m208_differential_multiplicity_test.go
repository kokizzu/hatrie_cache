package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestM208ConsolidateQuerySubscriptionDeltasFoldsEqualRows(t *testing.T) {
	first := Row{"id": "a", "value": "current"}
	second := Row{"id": "b", "value": "gone"}
	input := []QuerySubscriptionDelta{
		{Row: first, Diff: 2},
		{Row: second, Diff: -1},
		{Row: first, Diff: -1},
		{Row: second, Diff: 1},
		{Row: Row{"id": "c"}, Diff: 1},
		{Row: Row{"id": "c"}, Diff: -1},
	}
	got, err := ConsolidateQuerySubscriptionDeltas(input)
	if err != nil {
		t.Fatalf("ConsolidateQuerySubscriptionDeltas() error = %v", err)
	}
	want := []QuerySubscriptionDelta{{Row: first, Diff: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("consolidated = %#v, want %#v", got, want)
	}
	if input[0].Diff != 2 || input[0].Row["value"] != "current" {
		t.Fatalf("input was mutated: %#v", input)
	}
	got[0].Row["value"] = "changed"
	if first["value"] != "current" {
		t.Fatal("consolidated row aliases input row")
	}
}

func TestM208ConsolidateQuerySubscriptionDeltasRejectsOverflow(t *testing.T) {
	row := Row{"id": "overflow"}
	_, err := ConsolidateQuerySubscriptionDeltas([]QuerySubscriptionDelta{
		{Row: row, Diff: math.MaxInt64},
		{Row: row, Diff: 1},
	})
	if !errors.Is(err, ErrQuerySubscriptionDeltaOverflow) {
		t.Fatalf("error = %v, want ErrQuerySubscriptionDeltaOverflow", err)
	}
}

func TestM208DebeziumChangefeedFoldsRepeatedUpdateDeltas(t *testing.T) {
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	old := Row{"id": "a", "value": "old"}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{{Row: old, Diff: 1}}}); err != nil {
		t.Fatalf("Apply(initial) error = %v", err)
	}
	updated, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{
		{Row: old, Diff: -1},
		{Row: old, Diff: -1},
		{Row: old, Diff: 1},
		{Row: Row{"id": "a", "value": "new"}, Diff: 1},
	}})
	if err != nil {
		t.Fatalf("Apply(repeated update) error = %v", err)
	}
	if len(updated) != 1 || updated[0].Payload.Op != DebeziumUpdate {
		t.Fatalf("updated changes = %#v, want one update", updated)
	}
	if updated[0].Payload.Before["value"] != "old" || updated[0].Payload.After["value"] != "new" {
		t.Fatalf("updated images = %#v, want old -> new", updated[0].Payload)
	}
}

func TestM208DebeziumChangefeedConsolidationErrorIsAtomic(t *testing.T) {
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	row := Row{"id": "a"}
	_, err = feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{
		{Row: row, Diff: math.MaxInt64},
		{Row: row, Diff: 1},
	}})
	if !errors.Is(err, ErrQuerySubscriptionDeltaOverflow) {
		t.Fatalf("overflow error = %v, want ErrQuerySubscriptionDeltaOverflow", err)
	}
	changes, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{{Row: row, Diff: 1}}})
	if err != nil || len(changes) != 1 || changes[0].Payload.Op != DebeziumRead {
		t.Fatalf("Apply(after overflow) = %#v, error %v, want one snapshot read", changes, err)
	}
}
