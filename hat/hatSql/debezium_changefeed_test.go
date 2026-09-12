package hatSql

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDebeziumChangefeedEmitsSnapshotUpdateAndDelete(t *testing.T) {
	clock := time.UnixMilli(1700000000123)
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{
		KeyColumns: []string{"id"},
		Source:     DebeziumSource{Name: "orders", Connector: "hatrie-cache"},
		Clock:      func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}

	initial, err := feed.Apply(QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 1,
		Frontier: 10,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": "a", "status": "new"},
			Diff: 1,
		}},
	})
	if err != nil {
		t.Fatalf("Apply(initial) error = %v", err)
	}
	if len(initial) != 1 {
		t.Fatalf("Apply(initial) length = %d, want 1", len(initial))
	}
	if initial[0].Payload.Op != DebeziumRead || !initial[0].Payload.Source.Snapshot || initial[0].Payload.Before != nil {
		t.Fatalf("initial change = %#v, want snapshot read with nil before", initial[0])
	}
	if initial[0].Key["id"] != "a" || initial[0].Payload.After["status"] != "new" {
		t.Fatalf("initial change = %#v, want key and after image", initial[0])
	}
	if initial[0].Frontier != 10 || initial[0].Payload.TsMs != clock.UnixMilli() {
		t.Fatalf("initial metadata = %#v, want frontier 10 and fixed timestamp", initial[0])
	}

	updated, err := feed.Apply(QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 2,
		Frontier: 11,
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": "a", "status": "new"}, Diff: -1},
			{Row: Row{"id": "a", "status": "paid"}, Diff: 1},
		},
	})
	if err != nil {
		t.Fatalf("Apply(update) error = %v", err)
	}
	if len(updated) != 1 || updated[0].Payload.Op != DebeziumUpdate {
		t.Fatalf("update changes = %#v, want one update", updated)
	}
	if updated[0].Payload.Before["status"] != "new" || updated[0].Payload.After["status"] != "paid" {
		t.Fatalf("update images = %#v, want before=new and after=paid", updated[0].Payload)
	}
	if updated[0].Payload.Source.Snapshot {
		t.Fatalf("update source = %#v, want non-snapshot", updated[0].Payload.Source)
	}

	deleted, err := feed.Apply(QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 3,
		Frontier: 12,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": "a", "status": "paid"},
			Diff: -1,
		}},
	})
	if err != nil {
		t.Fatalf("Apply(delete) error = %v", err)
	}
	if len(deleted) != 1 || deleted[0].Payload.Op != DebeziumDelete || deleted[0].Payload.After != nil {
		t.Fatalf("delete changes = %#v, want one delete with nil after", deleted)
	}
	if deleted[0].Payload.Before["status"] != "paid" {
		t.Fatalf("delete before = %#v, want paid", deleted[0].Payload.Before)
	}

	encoded, err := json.Marshal(updated[0])
	if err != nil {
		t.Fatalf("json.Marshal(update) error = %v", err)
	}
	for _, token := range []string{`"key"`, `"before"`, `"after"`, `"op":"u"`, `"snapshot":false`} {
		if !strings.Contains(string(encoded), token) {
			t.Fatalf("encoded update = %s, missing %s", encoded, token)
		}
	}
}

func TestDebeziumChangefeedRequiresUniqueKeys(t *testing.T) {
	if _, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{}); !errors.Is(err, ErrDebeziumKeyColumnsRequired) {
		t.Fatalf("missing key columns error = %v, want %v", err, ErrDebeziumKeyColumnsRequired)
	}

	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	_, err = feed.Apply(QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": "a", "value": 1}, Diff: 1},
			{Row: Row{"id": "a", "value": 2}, Diff: 1},
		},
	})
	if !errors.Is(err, ErrDebeziumDuplicateKey) {
		t.Fatalf("duplicate key error = %v, want %v", err, ErrDebeziumDuplicateKey)
	}
}

func TestDebeziumChangefeedProgressDoesNotEmitData(t *testing.T) {
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	changes, err := feed.Apply(QuerySubscriptionDeltaBatch{Frontier: 20, Progress: true})
	if err != nil {
		t.Fatalf("Apply(progress) error = %v", err)
	}
	if changes != nil {
		t.Fatalf("Apply(progress) = %#v, want nil", changes)
	}
}

func TestDebeziumChangefeedResetAndValidationAreAtomic(t *testing.T) {
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{{Row: Row{"id": "a", "value": 1}, Diff: 1}},
	}); err != nil {
		t.Fatalf("Apply(initial) error = %v", err)
	}

	_, err = feed.Apply(QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": "a", "value": 1}, Diff: -1},
			{Row: Row{"id": "a", "value": 2}, Diff: -1},
		},
	})
	if !errors.Is(err, ErrDebeziumDuplicateKey) {
		t.Fatalf("invalid batch error = %v, want %v", err, ErrDebeziumDuplicateKey)
	}
	changes, err := feed.Apply(QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": "a", "value": 1}, Diff: -1},
			{Row: Row{"id": "b", "value": 3}, Diff: 1},
		},
	})
	if err != nil {
		t.Fatalf("Apply(after invalid) error = %v", err)
	}
	if len(changes) != 2 || changes[0].Payload.Op != DebeziumDelete || changes[1].Payload.Op != DebeziumCreate {
		t.Fatalf("changes after invalid batch = %#v, want delete/create", changes)
	}

	changes, err = feed.Apply(QuerySubscriptionDeltaBatch{
		Reset: true,
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": "b", "value": 4}, Diff: 1},
			{Row: Row{"id": "c", "value": 5}, Diff: 1},
		},
	})
	if err != nil {
		t.Fatalf("Apply(reset) error = %v", err)
	}
	if len(changes) != 2 || changes[0].Payload.Op != DebeziumUpdate || changes[1].Payload.Op != DebeziumCreate {
		t.Fatalf("reset changes = %#v, want update/create", changes)
	}
	if changes[0].Payload.Before["value"] != 3 || changes[0].Payload.After["value"] != 4 {
		t.Fatalf("reset update = %#v, want 3 -> 4", changes[0].Payload)
	}
}
