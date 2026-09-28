package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrUpsertEnvelopeInvalid reports a missing stable key, an invalid row/tombstone
// combination, or malformed JSON in an upsert envelope.
var ErrUpsertEnvelopeInvalid = errors.New("hatSql: invalid upsert envelope")

// UpsertEnvelope is the compact input form for a current-row change. Row maps
// are borrowed by NormalizeUpsertEnvelope and are not copied.
//
// A non-deleted envelope must contain Row. A deleted envelope is a tombstone
// and must leave Row nil.
type UpsertEnvelope struct {
	Sequence uint64 `json:"sequence,omitempty"`
	Key      string `json:"key"`
	Row      Row    `json:"row,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
}

// UpsertChange is a validated current-row change. Row is nil for tombstones.
// The returned row remains owned by the input envelope or JSON decoder.
type UpsertChange struct {
	Sequence uint64 `json:"sequence,omitempty"`
	Key      string `json:"key"`
	Row      Row    `json:"row,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
}

// NormalizeUpsertEnvelope validates an upsert envelope and canonicalizes its
// key. It does not copy the row map, keeping the successful path allocation-free.
func NormalizeUpsertEnvelope(envelope UpsertEnvelope) (UpsertChange, error) {
	key := strings.TrimSpace(envelope.Key)
	if key == "" {
		return UpsertChange{}, fmt.Errorf("%w: key is required", ErrUpsertEnvelopeInvalid)
	}
	if envelope.Deleted {
		if envelope.Row != nil {
			return UpsertChange{}, fmt.Errorf("%w: tombstone cannot contain a row", ErrUpsertEnvelopeInvalid)
		}
		return UpsertChange{Sequence: envelope.Sequence, Key: key, Deleted: true}, nil
	}
	if envelope.Row == nil {
		return UpsertChange{}, fmt.Errorf("%w: non-deleted envelope requires a row", ErrUpsertEnvelopeInvalid)
	}
	return UpsertChange{
		Sequence: envelope.Sequence,
		Key:      key,
		Row:      envelope.Row,
	}, nil
}

// DecodeUpsertEnvelopeJSON decodes and validates the canonical JSON envelope.
// A JSON null row is treated as nil and is valid only for a deleted envelope.
func DecodeUpsertEnvelopeJSON(data []byte) (UpsertChange, error) {
	var raw struct {
		Sequence uint64          `json:"sequence"`
		Key      string          `json:"key"`
		Row      json.RawMessage `json:"row"`
		Deleted  bool            `json:"deleted"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return UpsertChange{}, fmt.Errorf("%w: decode JSON: %v", ErrUpsertEnvelopeInvalid, err)
	}

	var row Row
	if len(raw.Row) > 0 && string(raw.Row) != "null" {
		if err := json.Unmarshal(raw.Row, &row); err != nil {
			return UpsertChange{}, fmt.Errorf("%w: decode row: %v", ErrUpsertEnvelopeInvalid, err)
		}
	}
	return NormalizeUpsertEnvelope(UpsertEnvelope{
		Sequence: raw.Sequence,
		Key:      raw.Key,
		Row:      row,
		Deleted:  raw.Deleted,
	})
}
