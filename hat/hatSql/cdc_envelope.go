package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	CDCOperationInsert = "INSERT"
	CDCOperationUpdate = "UPDATE"
	CDCOperationDelete = "DELETE"
)

// ErrCDCEnvelopeInvalid reports an unsupported operation or an invalid
// before/after/key combination in a CDC envelope.
var ErrCDCEnvelopeInvalid = errors.New("hatSql: invalid CDC envelope")

// CDCEnvelope is the input shape accepted from a change-data-capture source.
// Operation accepts common Debezium, Materialize, and Tarantool aliases.
// Rows are borrowed by NormalizeCDCEnvelope and are not modified.
type CDCEnvelope struct {
	Sequence  uint64 `json:"sequence,omitempty"`
	Operation string `json:"op,omitempty"`
	Key       string `json:"key"`
	Before    Row    `json:"before,omitempty"`
	After     Row    `json:"after,omitempty"`
}

// CDCChange is a validated CDC event with a canonical INSERT, UPDATE, or
// DELETE operation. Before is nil for INSERT, and After is nil for DELETE.
// Row maps are borrowed from the input envelope and remain caller-owned.
type CDCChange struct {
	Sequence  uint64 `json:"sequence,omitempty"`
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Before    Row    `json:"before,omitempty"`
	After     Row    `json:"after,omitempty"`
}

// NormalizeCDCEnvelope validates an external CDC event and canonicalizes its
// operation without copying row maps. The returned change is allocation-free
// on the successful path when the input operation and key need no new storage.
func NormalizeCDCEnvelope(envelope CDCEnvelope) (CDCChange, error) {
	operation, recognized := canonicalCDCOperation(envelope.Operation, envelope.Before, envelope.After)
	if !recognized {
		return CDCChange{}, fmt.Errorf("%w: unsupported operation %q", ErrCDCEnvelopeInvalid, strings.TrimSpace(envelope.Operation))
	}
	if operation == "" {
		return CDCChange{}, fmt.Errorf("%w: replace/upsert requires an after row", ErrCDCEnvelopeInvalid)
	}

	key := strings.TrimSpace(envelope.Key)
	if key == "" {
		return CDCChange{}, fmt.Errorf("%w: key is required", ErrCDCEnvelopeInvalid)
	}

	switch operation {
	case CDCOperationInsert:
		if envelope.Before != nil {
			return CDCChange{}, fmt.Errorf("%w: insert cannot have a before row", ErrCDCEnvelopeInvalid)
		}
		if envelope.After == nil {
			return CDCChange{}, fmt.Errorf("%w: insert requires an after row", ErrCDCEnvelopeInvalid)
		}
	case CDCOperationUpdate:
		if envelope.Before == nil {
			return CDCChange{}, fmt.Errorf("%w: update requires a before row", ErrCDCEnvelopeInvalid)
		}
		if envelope.After == nil {
			return CDCChange{}, fmt.Errorf("%w: update requires an after row", ErrCDCEnvelopeInvalid)
		}
	case CDCOperationDelete:
		if envelope.After != nil {
			return CDCChange{}, fmt.Errorf("%w: delete cannot have an after row", ErrCDCEnvelopeInvalid)
		}
	}

	return CDCChange{
		Sequence:  envelope.Sequence,
		Operation: operation,
		Key:       key,
		Before:    envelope.Before,
		After:     envelope.After,
	}, nil
}

// DecodeCDCEnvelopeJSON decodes a JSON CDC event and applies the same
// validation as NormalizeCDCEnvelope. Both "op" and "operation" field names
// are accepted; "op" takes precedence when both are semantically equivalent.
func DecodeCDCEnvelopeJSON(data []byte) (CDCChange, error) {
	var raw struct {
		Sequence  uint64 `json:"sequence"`
		Operation string `json:"op"`
		Alias     string `json:"operation"`
		Key       string `json:"key"`
		Before    Row    `json:"before"`
		After     Row    `json:"after"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return CDCChange{}, fmt.Errorf("%w: decode JSON: %v", ErrCDCEnvelopeInvalid, err)
	}

	operation := raw.Operation
	if strings.TrimSpace(operation) == "" {
		operation = raw.Alias
	} else if strings.TrimSpace(raw.Alias) != "" {
		first, firstRecognized := canonicalCDCOperation(raw.Operation, raw.Before, raw.After)
		second, secondRecognized := canonicalCDCOperation(raw.Alias, raw.Before, raw.After)
		if !firstRecognized || !secondRecognized || first == "" || second == "" || first != second {
			return CDCChange{}, fmt.Errorf("%w: op and operation disagree", ErrCDCEnvelopeInvalid)
		}
	}

	return NormalizeCDCEnvelope(CDCEnvelope{
		Sequence:  raw.Sequence,
		Operation: operation,
		Key:       raw.Key,
		Before:    raw.Before,
		After:     raw.After,
	})
}

func canonicalCDCOperation(operation string, before, after Row) (string, bool) {
	operation = strings.TrimSpace(operation)
	switch {
	case strings.EqualFold(operation, "i"),
		strings.EqualFold(operation, "c"),
		strings.EqualFold(operation, "create"),
		strings.EqualFold(operation, "insert"),
		strings.EqualFold(operation, "r"),
		strings.EqualFold(operation, "read"),
		strings.EqualFold(operation, "snapshot"):
		return CDCOperationInsert, true
	case strings.EqualFold(operation, "u"),
		strings.EqualFold(operation, "update"):
		return CDCOperationUpdate, true
	case strings.EqualFold(operation, "d"),
		strings.EqualFold(operation, "delete"),
		strings.EqualFold(operation, "remove"):
		return CDCOperationDelete, true
	case strings.EqualFold(operation, "replace"),
		strings.EqualFold(operation, "upsert"):
		switch {
		case before == nil && after != nil:
			return CDCOperationInsert, true
		case before != nil && after != nil:
			return CDCOperationUpdate, true
		default:
			return "", true
		}
	default:
		return "", false
	}
}
