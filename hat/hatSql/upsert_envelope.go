package hatSql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ErrUpsertEnvelopeInvalid reports a missing stable key, ambiguous image, or
// invalid row image in an upsert envelope.
var ErrUpsertEnvelopeInvalid = errors.New("hatSql: invalid upsert envelope")

// UpsertEnvelope is the current-state representation of one keyed row.
// A nil Row is a tombstone for Key; a non-nil Row is the current row image.
// Row maps are borrowed and are not modified by normalization or adapters.
type UpsertEnvelope struct {
	Sequence uint64 `json:"sequence,omitempty"`
	Revision uint64 `json:"revision,omitempty"`
	Frontier uint64 `json:"frontier,omitempty"`
	Key      string `json:"key"`
	Row      Row    `json:"row"`
}

// NormalizeUpsertEnvelope trims and validates the stable key without copying
// the current row image. This keeps the successful path allocation-free when
// the key is already canonical.
func NormalizeUpsertEnvelope(envelope UpsertEnvelope) (UpsertEnvelope, error) {
	key, err := normalizeUpsertKey(envelope.Key)
	if err != nil {
		return UpsertEnvelope{}, err
	}
	envelope.Key = key
	return envelope, nil
}

func normalizeUpsertKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: key is required", ErrUpsertEnvelopeInvalid)
	}
	// Most CDC keys are already canonical ASCII identifiers. Avoid the full
	// Unicode trim scan on that hot path while retaining strings.TrimSpace for
	// keys that may contain non-ASCII whitespace.
	if key[0] > ' ' && key[len(key)-1] > ' ' && key[0] < utf8.RuneSelf && key[len(key)-1] < utf8.RuneSelf {
		return key, nil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("%w: key is required", ErrUpsertEnvelopeInvalid)
	}
	return key, nil
}

// DecodeUpsertEnvelopeJSON decodes the canonical row field. The value and
// after aliases make the boundary convenient for common key/value and CDC
// producers; specifying more than one image field is rejected.
func DecodeUpsertEnvelopeJSON(data []byte) (UpsertEnvelope, error) {
	var raw struct {
		Sequence uint64          `json:"sequence"`
		Revision uint64          `json:"revision"`
		Frontier uint64          `json:"frontier"`
		Key      string          `json:"key"`
		Row      json.RawMessage `json:"row"`
		Value    json.RawMessage `json:"value"`
		After    json.RawMessage `json:"after"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return UpsertEnvelope{}, fmt.Errorf("%w: decode JSON: %v", ErrUpsertEnvelopeInvalid, err)
	}
	images := 0
	var imageData json.RawMessage
	for _, candidate := range []json.RawMessage{raw.Row, raw.Value, raw.After} {
		if len(candidate) == 0 {
			continue
		}
		images++
		imageData = candidate
	}
	if images > 1 {
		return UpsertEnvelope{}, fmt.Errorf("%w: row, value, and after are mutually exclusive", ErrUpsertEnvelopeInvalid)
	}
	var row Row
	if len(imageData) > 0 && !bytes.Equal(bytes.TrimSpace(imageData), []byte("null")) {
		if err := json.Unmarshal(imageData, &row); err != nil {
			return UpsertEnvelope{}, fmt.Errorf("%w: decode row image: %v", ErrUpsertEnvelopeInvalid, err)
		}
	}
	return NormalizeUpsertEnvelope(UpsertEnvelope{
		Sequence: raw.Sequence,
		Revision: raw.Revision,
		Frontier: raw.Frontier,
		Key:      raw.Key,
		Row:      row,
	})
}

// AsUpsertEnvelope converts a validated-or-canonical CDC change into its
// current-state form. Delete changes become tombstones.
func (change CDCChange) AsUpsertEnvelope() (UpsertEnvelope, error) {
	var row Row
	switch change.Operation {
	case CDCOperationInsert, CDCOperationUpdate:
		if change.After == nil {
			return UpsertEnvelope{}, fmt.Errorf("%w: %s requires an after row", ErrUpsertEnvelopeInvalid, change.Operation)
		}
		row = change.After
	case CDCOperationDelete:
		if change.After != nil {
			return UpsertEnvelope{}, fmt.Errorf("%w: delete cannot have an after row", ErrUpsertEnvelopeInvalid)
		}
	default:
		return UpsertEnvelope{}, fmt.Errorf("%w: unsupported CDC operation %q", ErrUpsertEnvelopeInvalid, change.Operation)
	}
	key := change.Key
	if key == "" {
		return UpsertEnvelope{}, fmt.Errorf("%w: key is required", ErrUpsertEnvelopeInvalid)
	}
	if key[0] <= ' ' || key[len(key)-1] <= ' ' || key[0] >= utf8.RuneSelf || key[len(key)-1] >= utf8.RuneSelf {
		var err error
		key, err = normalizeUpsertKey(key)
		if err != nil {
			return UpsertEnvelope{}, err
		}
	}
	return UpsertEnvelope{
		Sequence: change.Sequence,
		Key:      key,
		Row:      row,
	}, nil
}

// AsUpsertEnvelope converts a Debezium change into a current-state envelope.
// The deterministic serialization of the Debezium key row becomes the stable
// string key, while source position metadata is preserved.
func (change DebeziumChange) AsUpsertEnvelope() (UpsertEnvelope, error) {
	var row Row
	switch change.Payload.Op {
	case DebeziumCreate, DebeziumUpdate, DebeziumRead:
		if change.Payload.After == nil {
			return UpsertEnvelope{}, fmt.Errorf("%w: %s requires an after row", ErrUpsertEnvelopeInvalid, change.Payload.Op)
		}
		row = change.Payload.After
	case DebeziumDelete:
		// A delete is represented by the nil current image.
	default:
		return UpsertEnvelope{}, fmt.Errorf("%w: unsupported Debezium operation %q", ErrUpsertEnvelopeInvalid, change.Payload.Op)
	}
	key := change.StableKey
	if key == "" {
		key = querySubscriptionRowKey(change.Key)
	}
	key, err := normalizeUpsertKey(key)
	if err != nil {
		return UpsertEnvelope{}, err
	}
	return UpsertEnvelope{
		Sequence: change.ID,
		Revision: change.Revision,
		Frontier: change.Frontier,
		Key:      key,
		Row:      row,
	}, nil
}
