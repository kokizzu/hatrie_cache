package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ013DebeziumKafkaDecoderNormalizesOperations(t *testing.T) {
	decoder, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		key       string
		value     string
		operation KafkaTableOperation
		row       Row
		wantKey   string
	}{
		{
			name:      "create",
			key:       ` {"tenant":"acme", "id": 7} `,
			value:     `{"before":null,"after":{"id":7,"name":"Ada"},"op":"c"}`,
			operation: KafkaTableUpsert,
			row:       Row{"id": float64(7), "name": "Ada"},
			wantKey:   `{"id":7,"tenant":"acme"}`,
		},
		{
			name:      "update",
			key:       "customer-7",
			value:     `{"before":{"id":7,"name":"Ada"},"after":{"id":7,"name":"Grace"},"op":"u"}`,
			operation: KafkaTableUpsert,
			row:       Row{"id": float64(7), "name": "Grace"},
			wantKey:   "customer-7",
		},
		{
			name:      "snapshot read",
			key:       "customer-8",
			value:     `{"after":{"id":8},"op":"r"}`,
			operation: KafkaTableUpsert,
			row:       Row{"id": float64(8)},
			wantKey:   "customer-8",
		},
		{
			name:      "delete",
			key:       "customer-9",
			value:     `{"before":{"id":9},"after":null,"op":"d"}`,
			operation: KafkaTableDelete,
			wantKey:   "customer-9",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			change, err := decoder(KafkaTableMessage{Key: test.key, Value: []byte(test.value)})
			if err != nil {
				t.Fatalf("decode error = %v", err)
			}
			if change.Key != test.wantKey || change.Operation != test.operation {
				t.Fatalf("change = %#v, want key=%q operation=%d", change, test.wantKey, test.operation)
			}
			if !reflect.DeepEqual(change.Row, test.row) {
				t.Fatalf("row = %#v, want %#v", change.Row, test.row)
			}
		})
	}
}

func TestMZ013DebeziumKafkaDecoderSupportsSchemaPayloadAndTombstones(t *testing.T) {
	decoder, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	change, err := decoder(KafkaTableMessage{
		Key:   `{"id":10}`,
		Value: []byte(`{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":10},"op":"r"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if change.Operation != KafkaTableUpsert || change.Key != `{"id":10}` || change.Row["id"] != float64(10) {
		t.Fatalf("schema payload change = %#v", change)
	}
	tombstone, err := decoder(KafkaTableMessage{Key: "customer-10", Value: nil})
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.Key != "customer-10" || tombstone.Operation != KafkaTableDelete || tombstone.Row != nil {
		t.Fatalf("tombstone = %#v", tombstone)
	}
}

func TestMZ013DebeziumKafkaDecoderRejectsInvalidInput(t *testing.T) {
	decoder, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		key   string
		value string
		want  error
	}{
		{name: "missing key", value: `{"after":{"id":1},"op":"c"}`, want: ErrDebeziumKafkaKeyInvalid},
		{name: "unsupported operation", key: "1", value: `{"after":{"id":1},"op":"t"}`, want: ErrDebeziumKafkaEnvelopeInvalid},
		{name: "update without after", key: "1", value: `{"before":{"id":1},"op":"u"}`, want: ErrDebeziumKafkaEnvelopeInvalid},
		{name: "malformed JSON", key: "1", value: `{"after":`, want: ErrDebeziumKafkaEnvelopeInvalid},
		{name: "invalid key JSON", key: `{`, value: `{"after":{"id":1},"op":"c"}`, want: ErrDebeziumKafkaKeyInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decoder(KafkaTableMessage{Key: test.key, Value: []byte(test.value)})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, test.want)
			}
		})
	}
	if _, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{MaxPayloadBytes: MaxKafkaTableMessageBytes + 1}); !errors.Is(err, ErrDebeziumKafkaDecoderInvalid) {
		t.Fatalf("invalid payload limit error = %v", err)
	}
	decoder, err = NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{MaxPayloadBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder(KafkaTableMessage{Key: "1", Value: []byte(`{"op":"c"}`)}); !errors.Is(err, ErrDebeziumKafkaPayloadTooLarge) {
		t.Fatalf("large payload error = %v", err)
	}
}

func TestMZ013DebeziumKafkaDecoderIntegratesWithSource(t *testing.T) {
	decoder, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "debezium",
		Table:   "customers",
		Topic:   "customers",
		Decoder: decoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{Messages: []KafkaTableMessage{{
		Topic: "customers", Partition: "0", Offset: 1, Key: `{"id":11}`,
		Value: []byte(`{"after":{"id":11,"name":"Lin"},"op":"c"}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	rows, err := source.ResolveSQLSource("KAFKA", "customers")
	if err != nil || len(rows) != 1 || rows[0]["name"] != "Lin" {
		t.Fatalf("rows after create = %#v, err = %v", rows, err)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{Messages: []KafkaTableMessage{{
		Topic: "customers", Partition: "0", Offset: 2, Key: `{"id":11}`,
		Value: []byte(`{"before":{"id":11,"name":"Lin"},"after":null,"op":"d"}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	rows, err = source.ResolveSQLSource("KAFKA", "customers")
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows after delete = %#v, err = %v", rows, err)
	}
}
