package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM206UpsertChangefeedEmitsStableKeyAndCurrentImage(t *testing.T) {
	feed, err := NewUpsertChangefeed(UpsertChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}

	initial := QuerySubscriptionDeltaBatch{
		ID:       10,
		Revision: 11,
		Frontier: 12,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": int64(7), "name": "alice", "region": "sg"},
			Diff: 1,
		}},
	}
	events, err := feed.Apply(initial)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("initial event count = %d, want 1", len(events))
	}
	wantInitial := UpsertEnvelope{
		Key:      Row{"id": int64(7)},
		Current:  Row{"id": int64(7), "name": "alice", "region": "sg"},
		ID:       10,
		Revision: 11,
		Frontier: 12,
	}
	if !reflect.DeepEqual(events[0], wantInitial) {
		t.Fatalf("initial event = %#v, want %#v", events[0], wantInitial)
	}

	// The adapter must retain its own image so a consumer cannot corrupt the
	// next update by mutating a delivered envelope.
	events[0].Current["name"] = "consumer-mutated"
	update := QuerySubscriptionDeltaBatch{
		ID:       13,
		Revision: 14,
		Frontier: 15,
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(7), "name": "alice", "region": "sg"}, Diff: -1},
			{Row: Row{"id": int64(7), "name": "alice-2", "region": "sg"}, Diff: 1},
		},
	}
	events, err = feed.Apply(update)
	if err != nil {
		t.Fatal(err)
	}
	wantUpdate := UpsertEnvelope{
		Key:      Row{"id": int64(7)},
		Current:  Row{"id": int64(7), "name": "alice-2", "region": "sg"},
		ID:       13,
		Revision: 14,
		Frontier: 15,
	}
	if !reflect.DeepEqual(events, []UpsertEnvelope{wantUpdate}) {
		t.Fatalf("update events = %#v, want %#v", events, []UpsertEnvelope{wantUpdate})
	}

	deleteBatch := QuerySubscriptionDeltaBatch{
		ID:       16,
		Revision: 17,
		Frontier: 18,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": int64(7), "name": "alice-2", "region": "sg"},
			Diff: -1,
		}},
	}
	events, err = feed.Apply(deleteBatch)
	if err != nil {
		t.Fatal(err)
	}
	wantDelete := UpsertEnvelope{
		Key:      Row{"id": int64(7)},
		Deleted:  true,
		ID:       16,
		Revision: 17,
		Frontier: 18,
	}
	if !reflect.DeepEqual(events, []UpsertEnvelope{wantDelete}) {
		t.Fatalf("delete events = %#v, want %#v", events, []UpsertEnvelope{wantDelete})
	}
	if events[0].Current != nil {
		t.Fatalf("delete current image = %#v, want nil", events[0].Current)
	}
}

func TestM206UpsertChangefeedProgressResetAndStableOrdering(t *testing.T) {
	feed, err := NewUpsertChangefeed(UpsertChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if events, err := feed.Apply(QuerySubscriptionDeltaBatch{Progress: true}); err != nil || events != nil {
		t.Fatalf("progress result = %#v/%v, want nil/nil", events, err)
	}

	initial := QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(1), "value": "one"}, Diff: 1},
			{Row: Row{"id": int64(2), "value": "two"}, Diff: 1},
		},
	}
	if _, err := feed.Apply(initial); err != nil {
		t.Fatal(err)
	}
	reset := QuerySubscriptionDeltaBatch{
		ID:    20,
		Reset: true,
		Deltas: []QuerySubscriptionDelta{
			{Row: Row{"id": int64(1), "value": "one-updated"}, Diff: 1},
			{Row: Row{"id": int64(3), "value": "three"}, Diff: 1},
		},
	}
	events, err := feed.Apply(reset)
	if err != nil {
		t.Fatal(err)
	}
	want := []UpsertEnvelope{
		{Key: Row{"id": int64(1)}, Current: Row{"id": int64(1), "value": "one-updated"}, ID: 20},
		{Key: Row{"id": int64(3)}, Current: Row{"id": int64(3), "value": "three"}, ID: 20},
		{Key: Row{"id": int64(2)}, Deleted: true, ID: 20},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("reset events = %#v, want %#v", events, want)
	}
}

func TestM206UpsertChangefeedValidation(t *testing.T) {
	for name, options := range map[string]UpsertChangefeedOptions{
		"missing columns": {},
		"empty column":    {KeyColumns: []string{""}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewUpsertChangefeed(options); err == nil {
				t.Fatal("NewUpsertChangefeed() error = nil, want error")
			}
		})
	}

	feed, err := NewUpsertChangefeed(UpsertChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{{Row: Row{"id": int64(1)}, Diff: 2}}}); !errors.Is(err, ErrUpsertChangefeedUnsupportedMultiplicity) {
		t.Fatalf("initial multiplicity error = %v, want %v", err, ErrUpsertChangefeedUnsupportedMultiplicity)
	}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{
		{Row: Row{"id": int64(1), "value": "a"}, Diff: 1},
		{Row: Row{"id": int64(1), "value": "b"}, Diff: 1},
	}}); !errors.Is(err, ErrUpsertChangefeedDuplicateKey) {
		t.Fatalf("duplicate key error = %v, want %v", err, ErrUpsertChangefeedDuplicateKey)
	}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{{Row: Row{"id": int64(1), "value": "a"}, Diff: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Apply(QuerySubscriptionDeltaBatch{Deltas: []QuerySubscriptionDelta{{Row: Row{"id": int64(99)}, Diff: -1}}}); !errors.Is(err, ErrUpsertChangefeedUnknownDelete) {
		t.Fatalf("unknown delete error = %v, want %v", err, ErrUpsertChangefeedUnknownDelete)
	}
}

func TestM206UpsertChangefeedNilReceiver(t *testing.T) {
	var feed *UpsertChangefeed
	events, err := feed.Apply(QuerySubscriptionDeltaBatch{})
	if !errors.Is(err, ErrUpsertChangefeedNil) || events != nil {
		t.Fatalf("nil Apply() = %#v/%v, want nil/%v", events, err, ErrUpsertChangefeedNil)
	}
}
