package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM206NormalizeUpsertEnvelopeKeepsStableKeyAndCurrentImage(t *testing.T) {
	row := Row{"id": 7, "name": "current"}
	got, err := NormalizeUpsertEnvelope(UpsertEnvelope{
		Sequence: 42,
		Key:      " customer-7 ",
		Row:      row,
	})
	if err != nil {
		t.Fatalf("NormalizeUpsertEnvelope() error = %v", err)
	}
	if got.Sequence != 42 || got.Key != "customer-7" || !reflect.DeepEqual(got.Row, row) {
		t.Fatalf("envelope = %#v, want sequence/key/current image", got)
	}
	if got.Row["name"] != "current" {
		t.Fatal("current row image was not preserved")
	}
}

func TestM206DecodeUpsertEnvelopeJSONSupportsCanonicalAliasesAndTombstone(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		wantKey     string
		wantDeleted bool
	}{
		{name: "row", data: `{"sequence":9,"key":" 7 ","row":{"id":7}}`, wantKey: "7"},
		{name: "value alias", data: `{"key":"7","value":{"id":7}}`, wantKey: "7"},
		{name: "after alias", data: `{"key":"7","after":{"id":7}}`, wantKey: "7"},
		{name: "tombstone", data: `{"key":"7","row":null}`, wantKey: "7", wantDeleted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeUpsertEnvelopeJSON([]byte(test.data))
			if err != nil {
				t.Fatalf("DecodeUpsertEnvelopeJSON() error = %v", err)
			}
			if got.Key != test.wantKey || (got.Row == nil) != test.wantDeleted {
				t.Fatalf("envelope = %#v, want key=%q deleted=%v", got, test.wantKey, test.wantDeleted)
			}
		})
	}
}

func TestM206UpsertEnvelopeRejectsInvalidOrAmbiguousInput(t *testing.T) {
	if _, err := NormalizeUpsertEnvelope(UpsertEnvelope{Row: Row{"id": 1}}); !errors.Is(err, ErrUpsertEnvelopeInvalid) {
		t.Fatalf("missing key error = %v, want ErrUpsertEnvelopeInvalid", err)
	}
	if _, err := DecodeUpsertEnvelopeJSON([]byte(`{"key":"7","row":{"id":7},"value":{"id":7}}`)); !errors.Is(err, ErrUpsertEnvelopeInvalid) {
		t.Fatalf("ambiguous image error = %v, want ErrUpsertEnvelopeInvalid", err)
	}
	if _, err := DecodeUpsertEnvelopeJSON([]byte(`{"key":"7","row":[]}`)); !errors.Is(err, ErrUpsertEnvelopeInvalid) {
		t.Fatalf("invalid row error = %v, want ErrUpsertEnvelopeInvalid", err)
	}
}

func TestM206CDCAndDebeziumChangesConvertToUpsertEnvelope(t *testing.T) {
	row := Row{"id": 7, "name": "after"}
	cdc, err := (CDCChange{Sequence: 11, Operation: CDCOperationUpdate, Key: "7", After: row}).AsUpsertEnvelope()
	if err != nil || cdc.Key != "7" || !reflect.DeepEqual(cdc.Row, row) {
		t.Fatalf("CDC conversion = %#v, error %v", cdc, err)
	}
	deleted, err := (CDCChange{Sequence: 12, Operation: CDCOperationDelete, Key: "7"}).AsUpsertEnvelope()
	if err != nil || deleted.Key != "7" || deleted.Row != nil {
		t.Fatalf("CDC tombstone = %#v, error %v", deleted, err)
	}

	debezium := DebeziumChange{
		Key:       Row{"tenant": "a", "id": 7},
		StableKey: "cached-customer-7",
		Payload:   DebeziumPayload{Op: DebeziumUpdate, Before: Row{"id": 7}, After: row},
	}
	first, err := debezium.AsUpsertEnvelope()
	if err != nil || first.Row["name"] != "after" || first.Key != "cached-customer-7" {
		t.Fatalf("Debezium conversion = %#v, error %v", first, err)
	}
	debezium.Key = Row{"id": 7, "tenant": "a"}
	second, err := debezium.AsUpsertEnvelope()
	if err != nil || second.Key != first.Key {
		t.Fatalf("Debezium key stability = %q/%q, error %v", first.Key, second.Key, err)
	}
}

func TestM206DebeziumChangefeedCarriesCanonicalStableKey(t *testing.T) {
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatalf("NewDebeziumChangefeed() error = %v", err)
	}
	changes, err := feed.Apply(QuerySubscriptionDeltaBatch{
		Deltas: []QuerySubscriptionDelta{{Diff: 1, Row: Row{"id": 7, "value": "current"}}},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	want := querySubscriptionRowKey(changes[0].Key)
	if changes[0].StableKey != want || changes[0].StableKey == "" {
		t.Fatalf("stable key = %q, want canonical key %q", changes[0].StableKey, want)
	}
}
