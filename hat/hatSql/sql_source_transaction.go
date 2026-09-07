package hatSql

import (
	"errors"
	"strings"
)

var (
	// ErrSQLSourceTransactionInvalid reports a missing transaction ID or empty
	// offset group.
	ErrSQLSourceTransactionInvalid = errors.New("SQL source transaction is invalid")
)

// SQLSourceTransaction groups source partition offsets that must become
// visible together. The ID is source-supplied metadata; durable transaction
// deduplication remains the responsibility of a higher-level exactly-once
// consumer.
type SQLSourceTransaction struct {
	ID      string            `json:"id"`
	Offsets []SQLSourceOffset `json:"offsets"`
}

// AdvanceTransaction atomically advances all offsets in transaction. A
// transaction is accepted only when every member is newer than its current
// high-watermark. If any member is stale, no member is changed and the method
// returns false without error.
func (tracker *SQLSourceOffsetTracker) AdvanceTransaction(transaction SQLSourceTransaction) (bool, error) {
	if tracker == nil {
		return false, ErrSQLSourceOffsetTrackerNil
	}
	if strings.TrimSpace(transaction.ID) == "" || len(transaction.Offsets) == 0 {
		return false, ErrSQLSourceTransactionInvalid
	}

	normalized := make([]SQLSourceOffset, len(transaction.Offsets))
	keys := make(map[sqlSourceOffsetKey]struct{}, len(transaction.Offsets))
	for index, offset := range transaction.Offsets {
		key, value, err := normalizeSQLSourceOffset(offset)
		if err != nil {
			return false, err
		}
		if _, found := keys[key]; found {
			return false, ErrSQLSourceOffsetDuplicate
		}
		keys[key] = struct{}{}
		normalized[index] = value
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	for _, offset := range normalized {
		key := sqlSourceOffsetKey{source: offset.Source, partition: offset.Partition}
		if current, found := tracker.offsets[key]; found && offset.Offset <= current {
			return false, nil
		}
	}
	tracker.ensureMapLocked()
	for _, offset := range normalized {
		key := sqlSourceOffsetKey{source: offset.Source, partition: offset.Partition}
		tracker.offsets[key] = offset.Offset
	}
	return true, nil
}
