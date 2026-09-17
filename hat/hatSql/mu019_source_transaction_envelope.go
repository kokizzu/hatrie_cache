package hatSql

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// DefaultSQLSourceTransactionEnvelopeMaxRelations bounds the number of
	// relation labels accepted in one source transaction envelope.
	DefaultSQLSourceTransactionEnvelopeMaxRelations = 1024
	// MaxSQLSourceTransactionEnvelopeRelationNameBytes bounds one relation
	// label so untrusted source metadata cannot retain arbitrarily large data.
	MaxSQLSourceTransactionEnvelopeRelationNameBytes = 256
)

// ErrSQLSourceTransactionEnvelopeInvalid reports a malformed cross-relation
// source transaction envelope. It aliases the legacy ingestion validation
// error so callers can use one validation check across both APIs.
var ErrSQLSourceTransactionEnvelopeInvalid = ErrSQLSourceIngestionInvalid

// SQLSourceTransactionEnvelope identifies one source transaction whose
// changes span one or more named relations. The source adapter applies all
// relation changes in one callback; the coordinator records the commit marker
// only after that callback returns nil.
type SQLSourceTransactionEnvelope struct {
	Source      string               `json:"source"`
	Transaction SQLSourceTransaction `json:"transaction"`
	Relations   []string             `json:"relations"`
}

// SQLSourceTransactionEnvelopeCoordinator is the descriptive name for the
// existing coordinator when it is used with cross-relation envelopes.
type SQLSourceTransactionEnvelopeCoordinator = SQLSourceIngestionCoordinator

// NewSQLSourceTransactionEnvelopeCoordinator creates a coordinator for
// cross-relation source transaction envelopes.
func NewSQLSourceTransactionEnvelopeCoordinator() *SQLSourceTransactionEnvelopeCoordinator {
	return NewSQLSourceIngestionCoordinator()
}

func normalizeSQLSourceTransactionEnvelope(envelope SQLSourceTransactionEnvelope, requireRelations bool) (sqlSourceIngestionKey, SQLSourceTransactionEnvelope, error) {
	envelope.Source = strings.TrimSpace(envelope.Source)
	envelope.Transaction.ID = strings.TrimSpace(envelope.Transaction.ID)
	if envelope.Source == "" || envelope.Transaction.ID == "" || len(envelope.Transaction.Offsets) == 0 {
		return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
	}

	normalizedOffsets := make([]SQLSourceOffset, len(envelope.Transaction.Offsets))
	seenOffsets := make(map[sqlSourceOffsetKey]struct{}, len(envelope.Transaction.Offsets))
	for index, offset := range envelope.Transaction.Offsets {
		key, value, err := normalizeSQLSourceOffset(offset)
		if err != nil {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, fmt.Errorf("%w: %w", ErrSQLSourceTransactionEnvelopeInvalid, err)
		}
		if value.Source != envelope.Source {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
		}
		if _, found := seenOffsets[key]; found {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, fmt.Errorf("%w: %w", ErrSQLSourceTransactionEnvelopeInvalid, ErrSQLSourceOffsetDuplicate)
		}
		seenOffsets[key] = struct{}{}
		normalizedOffsets[index] = value
	}
	sort.Slice(normalizedOffsets, func(left, right int) bool {
		return normalizedOffsets[left].Partition < normalizedOffsets[right].Partition
	})

	if len(envelope.Relations) == 0 {
		if requireRelations {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
		}
		envelope.Transaction.Offsets = normalizedOffsets
		return sqlSourceIngestionKey{source: envelope.Source, transactionID: envelope.Transaction.ID}, envelope, nil
	}
	if len(envelope.Relations) > DefaultSQLSourceTransactionEnvelopeMaxRelations {
		return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
	}
	normalizedRelations := make([]string, len(envelope.Relations))
	for index, relation := range envelope.Relations {
		relation = strings.TrimSpace(relation)
		if relation == "" || len(relation) > MaxSQLSourceTransactionEnvelopeRelationNameBytes || strings.IndexByte(relation, 0) >= 0 {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
		}
		normalizedRelations[index] = relation
	}
	sort.Strings(normalizedRelations)
	for index := 1; index < len(normalizedRelations); index++ {
		if normalizedRelations[index] == normalizedRelations[index-1] {
			return sqlSourceIngestionKey{}, SQLSourceTransactionEnvelope{}, ErrSQLSourceTransactionEnvelopeInvalid
		}
	}
	envelope.Transaction.Offsets = normalizedOffsets
	envelope.Relations = normalizedRelations
	return sqlSourceIngestionKey{source: envelope.Source, transactionID: envelope.Transaction.ID}, envelope, nil
}

func equalSQLSourceTransactionMetadata(left, right SQLSourceTransactionEnvelope) bool {
	return equalSQLSourceOffsets(left.Transaction.Offsets, right.Transaction.Offsets) && equalStringSlices(left.Relations, right.Relations)
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
