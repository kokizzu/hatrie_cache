package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestNormalizeCDCEnvelopeCanonicalizesCommonOperations(t *testing.T) {
	before := Row{"id": 7, "name": "before"}
	after := Row{"id": 7, "name": "after"}
	tests := []struct {
		name      string
		operation string
		before    Row
		after     Row
		want      string
	}{
		{name: "create alias", operation: " c ", after: after, want: CDCOperationInsert},
		{name: "snapshot alias", operation: "R", after: after, want: CDCOperationInsert},
		{name: "update", operation: "UPDATE", before: before, after: after, want: CDCOperationUpdate},
		{name: "delete", operation: "d", before: before, want: CDCOperationDelete},
		{name: "key only delete", operation: "remove", want: CDCOperationDelete},
		{name: "replace insert", operation: "replace", after: after, want: CDCOperationInsert},
		{name: "upsert update", operation: "UPSERT", before: before, after: after, want: CDCOperationUpdate},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeCDCEnvelope(CDCEnvelope{
				Sequence:  42,
				Operation: test.operation,
				Key:       " customer-7 ",
				Before:    test.before,
				After:     test.after,
			})
			if err != nil {
				t.Fatalf("NormalizeCDCEnvelope() error = %v", err)
			}
			if got.Operation != test.want {
				t.Fatalf("operation = %q, want %q", got.Operation, test.want)
			}
			if got.Sequence != 42 {
				t.Fatalf("sequence = %d, want 42", got.Sequence)
			}
			if got.Key != "customer-7" {
				t.Fatalf("key = %q, want customer-7", got.Key)
			}
			if !reflect.DeepEqual(got.Before, test.before) {
				t.Fatalf("before = %#v, want %#v", got.Before, test.before)
			}
			if !reflect.DeepEqual(got.After, test.after) {
				t.Fatalf("after = %#v, want %#v", got.After, test.after)
			}
		})
	}
}

func TestNormalizeCDCEnvelopeRejectsInvalidShapes(t *testing.T) {
	row := Row{"id": 7}
	tests := []struct {
		name     string
		envelope CDCEnvelope
	}{
		{name: "missing operation", envelope: CDCEnvelope{Key: "7", After: row}},
		{name: "unsupported operation", envelope: CDCEnvelope{Operation: "merge", Key: "7", After: row}},
		{name: "missing key", envelope: CDCEnvelope{Operation: "insert", After: row}},
		{name: "insert before", envelope: CDCEnvelope{Operation: "insert", Key: "7", Before: row, After: row}},
		{name: "insert after", envelope: CDCEnvelope{Operation: "insert", Key: "7"}},
		{name: "update before", envelope: CDCEnvelope{Operation: "update", Key: "7", After: row}},
		{name: "update after", envelope: CDCEnvelope{Operation: "update", Key: "7", Before: row}},
		{name: "delete after", envelope: CDCEnvelope{Operation: "delete", Key: "7", After: row}},
		{name: "replace without row", envelope: CDCEnvelope{Operation: "replace", Key: "7"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NormalizeCDCEnvelope(test.envelope)
			if !errors.Is(err, ErrCDCEnvelopeInvalid) {
				t.Fatalf("error = %v, want errors.Is(..., ErrCDCEnvelopeInvalid)", err)
			}
		})
	}
}

func TestDecodeCDCEnvelopeJSONSupportsOperationAliases(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "debezium op",
			data: `{"sequence":17,"op":"u","key":"7","before":{"id":7},"after":{"id":8}}`,
			want: CDCOperationUpdate,
		},
		{
			name: "canonical operation",
			data: `{"operation":"r","key":"7","after":{"id":7}}`,
			want: CDCOperationInsert,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeCDCEnvelopeJSON([]byte(test.data))
			if err != nil {
				t.Fatalf("DecodeCDCEnvelopeJSON() error = %v", err)
			}
			if got.Operation != test.want {
				t.Fatalf("operation = %q, want %q", got.Operation, test.want)
			}
			if got.Sequence != 17 && test.name == "debezium op" {
				t.Fatalf("sequence = %d, want 17", got.Sequence)
			}
		})
	}
}

func TestDecodeCDCEnvelopeJSONRejectsMalformedInput(t *testing.T) {
	_, err := DecodeCDCEnvelopeJSON([]byte(`{"op":"insert","key":"7","after":`))
	if !errors.Is(err, ErrCDCEnvelopeInvalid) {
		t.Fatalf("error = %v, want errors.Is(..., ErrCDCEnvelopeInvalid)", err)
	}
}
