package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultSQLConnectorTransactionRetryJournalCapacity bounds the number of
	// transaction records retained by a journal when no capacity is supplied.
	DefaultSQLConnectorTransactionRetryJournalCapacity = 1024
	// MaxSQLConnectorTransactionRetryJournalCapacity bounds untrusted journal
	// configuration and keeps restore allocations predictable.
	MaxSQLConnectorTransactionRetryJournalCapacity = 65536
	// MaxSQLConnectorTransactionRetryJournalStringBytes bounds source, ID,
	// partition, and relation metadata retained in one record.
	MaxSQLConnectorTransactionRetryJournalStringBytes = 256
	// MaxSQLConnectorTransactionRetryJournalErrorCodeBytes bounds the safe
	// caller-provided error category retained by a record.
	MaxSQLConnectorTransactionRetryJournalErrorCodeBytes = 128
	// MaxSQLConnectorTransactionRetryJournalOffsets bounds source partitions in
	// one transaction record.
	MaxSQLConnectorTransactionRetryJournalOffsets = 1024
	// MaxSQLConnectorTransactionRetryJournalSnapshotBytes bounds binary restore
	// input before parsing any of its variable-length fields.
	MaxSQLConnectorTransactionRetryJournalSnapshotBytes = 16 << 20
)

var (
	// ErrSQLConnectorTransactionRetryJournalNil reports a nil journal receiver.
	ErrSQLConnectorTransactionRetryJournalNil = errors.New("SQL connector transaction retry journal is nil")
	// ErrSQLConnectorTransactionRetryJournalInvalid reports malformed metadata,
	// options, snapshots, or binary payloads.
	ErrSQLConnectorTransactionRetryJournalInvalid = errors.New("SQL connector transaction retry journal is invalid")
	// ErrSQLConnectorTransactionRetryJournalConflict reports a transaction
	// identity or attempt mismatch.
	ErrSQLConnectorTransactionRetryJournalConflict = errors.New("SQL connector transaction retry journal conflict")
	// ErrSQLConnectorTransactionRetryJournalCapacity reports that all retained
	// records are still in flight and cannot be evicted.
	ErrSQLConnectorTransactionRetryJournalCapacity = errors.New("SQL connector transaction retry journal is at capacity")
	// ErrSQLConnectorTransactionRetryJournalNotFound reports an unknown
	// transaction completion or lookup target.
	ErrSQLConnectorTransactionRetryJournalNotFound = errors.New("SQL connector transaction retry journal record is not found")
	// ErrSQLConnectorTransactionRetryJournalState reports an illegal state
	// transition or completion outcome.
	ErrSQLConnectorTransactionRetryJournalState = errors.New("SQL connector transaction retry journal state transition is invalid")
)

// SQLConnectorTransactionRetryJournalOptions configures a bounded retry
// journal. Capacity zero selects the default.
type SQLConnectorTransactionRetryJournalOptions struct {
	Capacity int `json:"capacity,omitempty"`
}

// SQLConnectorTransactionRetryJournalState is the durable lifecycle state of
// one connector transaction.
type SQLConnectorTransactionRetryJournalState string

const (
	SQLConnectorTransactionPending   SQLConnectorTransactionRetryJournalState = "pending"
	SQLConnectorTransactionRetryable SQLConnectorTransactionRetryJournalState = "retryable"
	SQLConnectorTransactionCommitted SQLConnectorTransactionRetryJournalState = "committed"
	SQLConnectorTransactionAborted   SQLConnectorTransactionRetryJournalState = "aborted"
)

// SQLConnectorTransactionBeginResult describes the idempotent result of
// beginning a transaction.
type SQLConnectorTransactionBeginResult string

const (
	SQLConnectorTransactionStarted          SQLConnectorTransactionBeginResult = "started"
	SQLConnectorTransactionRetryStarted     SQLConnectorTransactionBeginResult = "retry_started"
	SQLConnectorTransactionInProgress       SQLConnectorTransactionBeginResult = "in_progress"
	SQLConnectorTransactionAlreadyCommitted SQLConnectorTransactionBeginResult = "already_committed"
	SQLConnectorTransactionAlreadyAborted   SQLConnectorTransactionBeginResult = "already_aborted"
	SQLConnectorTransactionBeginInvalid     SQLConnectorTransactionBeginResult = "invalid"
)

// SQLConnectorTransactionCompletion acknowledges the outcome of one journal
// attempt. ErrorCode is a bounded category, never a raw error message.
type SQLConnectorTransactionCompletion struct {
	Source        string                                   `json:"source"`
	TransactionID string                                   `json:"transaction_id"`
	Attempt       uint64                                   `json:"attempt"`
	Outcome       SQLConnectorTransactionRetryJournalState `json:"outcome"`
	ErrorCode     string                                   `json:"error_code,omitempty"`
}

// SQLConnectorTransactionRecord is the independently owned durable journal
// state for one source transaction.
type SQLConnectorTransactionRecord struct {
	Sequence      uint64                                   `json:"sequence"`
	Envelope      SQLSourceTransactionEnvelope             `json:"envelope"`
	State         SQLConnectorTransactionRetryJournalState `json:"state"`
	Attempts      uint64                                   `json:"attempts"`
	LastErrorCode string                                   `json:"last_error_code,omitempty"`
	UpdatedAt     time.Time                                `json:"updated_at,omitempty"`
}

// SQLConnectorTransactionRetryJournalSnapshot is a deterministic, portable
// checkpoint of a retry journal. Records are ordered by insertion sequence.
type SQLConnectorTransactionRetryJournalSnapshot struct {
	Capacity     int                             `json:"capacity"`
	Dropped      uint64                          `json:"dropped"`
	NextSequence uint64                          `json:"next_sequence"`
	Records      []SQLConnectorTransactionRecord `json:"records"`
}

// SQLConnectorTransactionRetryJournalStats reports bounded retention state.
type SQLConnectorTransactionRetryJournalStats struct {
	Capacity     int    `json:"capacity"`
	Retained     int    `json:"retained"`
	Dropped      uint64 `json:"dropped"`
	NextSequence uint64 `json:"next_sequence"`
}

type sqlConnectorTransactionRetryJournalKey struct {
	source        string
	transactionID string
}

// SQLConnectorTransactionRetryJournal records transaction intent before a
// connector applies it, then fences retries by attempt. It is safe for
// concurrent callers. The journal is opt-in; existing source offset APIs are
// unchanged.
type SQLConnectorTransactionRetryJournal struct {
	mu           sync.RWMutex
	capacity     int
	records      map[sqlConnectorTransactionRetryJournalKey]SQLConnectorTransactionRecord
	order        []sqlConnectorTransactionRetryJournalKey
	dropped      uint64
	nextSequence uint64
}

// NewSQLConnectorTransactionRetryJournal creates an empty bounded journal.
func NewSQLConnectorTransactionRetryJournal(options SQLConnectorTransactionRetryJournalOptions) (*SQLConnectorTransactionRetryJournal, error) {
	capacity, err := normalizeSQLConnectorTransactionRetryJournalCapacity(options.Capacity)
	if err != nil {
		return nil, err
	}
	return &SQLConnectorTransactionRetryJournal{
		capacity:     capacity,
		records:      make(map[sqlConnectorTransactionRetryJournalKey]SQLConnectorTransactionRecord, capacity),
		order:        make([]sqlConnectorTransactionRetryJournalKey, 0, capacity),
		nextSequence: 1,
	}, nil
}

// NewSQLConnectorTransactionRetryJournalFromSnapshot restores a journal from
// a validated snapshot without exposing partially restored state.
func NewSQLConnectorTransactionRetryJournalFromSnapshot(snapshot SQLConnectorTransactionRetryJournalSnapshot) (*SQLConnectorTransactionRetryJournal, error) {
	j, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: snapshot.Capacity})
	if err != nil {
		return nil, err
	}
	if err := j.Restore(snapshot); err != nil {
		return nil, err
	}
	return j, nil
}

// Begin durably records transaction intent before the connector applies the
// transaction. A retryable record receives the next attempt number; pending,
// committed, and aborted records are idempotent results.
func (j *SQLConnectorTransactionRetryJournal) Begin(envelope SQLSourceTransactionEnvelope) (SQLConnectorTransactionRecord, SQLConnectorTransactionBeginResult, error) {
	if j == nil {
		return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalNil
	}
	key, normalized, err := normalizeSQLConnectorTransactionRetryJournalEnvelope(envelope)
	if err != nil {
		return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, err
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if existing, found := j.records[key]; found {
		if !equalSQLSourceTransactionMetadata(existing.Envelope, normalized) {
			return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalConflict
		}
		switch existing.State {
		case SQLConnectorTransactionPending:
			return cloneSQLConnectorTransactionRetryJournalRecord(existing), SQLConnectorTransactionInProgress, nil
		case SQLConnectorTransactionRetryable:
			if existing.Attempts == ^uint64(0) {
				return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			existing.Attempts++
			existing.State = SQLConnectorTransactionPending
			existing.LastErrorCode = ""
			existing.UpdatedAt = sqlConnectorTransactionRetryJournalNow()
			j.records[key] = existing
			return cloneSQLConnectorTransactionRetryJournalRecord(existing), SQLConnectorTransactionRetryStarted, nil
		case SQLConnectorTransactionCommitted:
			return cloneSQLConnectorTransactionRetryJournalRecord(existing), SQLConnectorTransactionAlreadyCommitted, nil
		case SQLConnectorTransactionAborted:
			return cloneSQLConnectorTransactionRetryJournalRecord(existing), SQLConnectorTransactionAlreadyAborted, nil
		default:
			return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalInvalid
		}
	}
	if len(j.records) >= j.capacity && !j.evictOldestTerminalLocked() {
		return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalCapacity
	}
	if j.nextSequence == 0 {
		return SQLConnectorTransactionRecord{}, SQLConnectorTransactionBeginInvalid, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	record := SQLConnectorTransactionRecord{
		Sequence:  j.nextSequence,
		Envelope:  normalized,
		State:     SQLConnectorTransactionPending,
		Attempts:  1,
		UpdatedAt: sqlConnectorTransactionRetryJournalNow(),
	}
	j.nextSequence++
	j.records[key] = record
	j.order = append(j.order, key)
	return cloneSQLConnectorTransactionRetryJournalRecord(record), SQLConnectorTransactionStarted, nil
}

// Complete records a terminal or retryable outcome for an exact attempt.
// Repeating the same terminal completion is idempotent; mismatched attempts
// cannot mutate the record.
func (j *SQLConnectorTransactionRetryJournal) Complete(completion SQLConnectorTransactionCompletion) (SQLConnectorTransactionRecord, error) {
	if j == nil {
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalNil
	}
	key, normalized, err := normalizeSQLConnectorTransactionRetryJournalCompletion(completion)
	if err != nil {
		return SQLConnectorTransactionRecord{}, err
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	record, found := j.records[key]
	if !found {
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalNotFound
	}
	if normalized.Attempt != record.Attempts {
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalConflict
	}
	switch record.State {
	case SQLConnectorTransactionPending:
		record.State = normalized.Outcome
		record.LastErrorCode = normalized.ErrorCode
		record.UpdatedAt = sqlConnectorTransactionRetryJournalNow()
		j.records[key] = record
		return cloneSQLConnectorTransactionRetryJournalRecord(record), nil
	case SQLConnectorTransactionRetryable:
		if normalized.Outcome == SQLConnectorTransactionRetryable && normalized.ErrorCode == record.LastErrorCode {
			return cloneSQLConnectorTransactionRetryJournalRecord(record), nil
		}
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalState
	case SQLConnectorTransactionCommitted:
		if normalized.Outcome == SQLConnectorTransactionCommitted {
			return cloneSQLConnectorTransactionRetryJournalRecord(record), nil
		}
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalState
	case SQLConnectorTransactionAborted:
		if normalized.Outcome == SQLConnectorTransactionAborted && normalized.ErrorCode == record.LastErrorCode {
			return cloneSQLConnectorTransactionRetryJournalRecord(record), nil
		}
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalState
	default:
		return SQLConnectorTransactionRecord{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
}

// Lookup returns a defensive copy of a transaction record.
func (j *SQLConnectorTransactionRetryJournal) Lookup(source, transactionID string) (SQLConnectorTransactionRecord, bool) {
	if j == nil {
		return SQLConnectorTransactionRecord{}, false
	}
	source = strings.TrimSpace(source)
	transactionID = strings.TrimSpace(transactionID)
	if validateSQLConnectorTransactionRetryJournalText(source, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil || validateSQLConnectorTransactionRetryJournalText(transactionID, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil {
		return SQLConnectorTransactionRecord{}, false
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	record, found := j.records[sqlConnectorTransactionRetryJournalKey{source: source, transactionID: transactionID}]
	if !found {
		return SQLConnectorTransactionRecord{}, false
	}
	return cloneSQLConnectorTransactionRetryJournalRecord(record), true
}

// Stats returns bounded retention counters without exposing internal slices.
func (j *SQLConnectorTransactionRetryJournal) Stats() SQLConnectorTransactionRetryJournalStats {
	if j == nil {
		return SQLConnectorTransactionRetryJournalStats{}
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return SQLConnectorTransactionRetryJournalStats{
		Capacity:     j.capacity,
		Retained:     len(j.records),
		Dropped:      j.dropped,
		NextSequence: j.nextSequence,
	}
}

// Snapshot returns a deterministic, independently owned checkpoint.
func (j *SQLConnectorTransactionRetryJournal) Snapshot() SQLConnectorTransactionRetryJournalSnapshot {
	if j == nil {
		return SQLConnectorTransactionRetryJournalSnapshot{}
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	snapshot := SQLConnectorTransactionRetryJournalSnapshot{
		Capacity:     j.capacity,
		Dropped:      j.dropped,
		NextSequence: j.nextSequence,
		Records:      make([]SQLConnectorTransactionRecord, 0, len(j.order)),
	}
	for _, key := range j.order {
		if record, found := j.records[key]; found {
			snapshot.Records = append(snapshot.Records, cloneSQLConnectorTransactionRetryJournalRecord(record))
		}
	}
	return snapshot
}

// Restore atomically replaces the journal with a validated snapshot. An
// invalid snapshot leaves the current journal untouched.
func (j *SQLConnectorTransactionRetryJournal) Restore(snapshot SQLConnectorTransactionRetryJournalSnapshot) error {
	if j == nil {
		return ErrSQLConnectorTransactionRetryJournalNil
	}
	replacement, order, capacity, err := validateSQLConnectorTransactionRetryJournalSnapshot(snapshot)
	if err != nil {
		return err
	}
	j.mu.Lock()
	j.records = replacement
	j.order = order
	j.capacity = capacity
	j.dropped = snapshot.Dropped
	j.nextSequence = snapshot.NextSequence
	j.mu.Unlock()
	return nil
}

// MarshalBinary encodes a compact CRC-protected journal checkpoint. Only the
// bounded error category is serialized; raw error messages are not accepted
// by this API and cannot enter the payload.
func (j *SQLConnectorTransactionRetryJournal) MarshalBinary() ([]byte, error) {
	if j == nil {
		return nil, ErrSQLConnectorTransactionRetryJournalNil
	}
	snapshot := j.Snapshot()
	return marshalSQLConnectorTransactionRetryJournalSnapshot(snapshot)
}

// UnmarshalSQLConnectorTransactionRetryJournal decodes and validates a binary
// journal checkpoint.
func UnmarshalSQLConnectorTransactionRetryJournal(data []byte) (SQLConnectorTransactionRetryJournalSnapshot, error) {
	if len(data) < len(sqlConnectorTransactionRetryJournalMagic)+1+4 || len(data) > MaxSQLConnectorTransactionRetryJournalSnapshotBytes {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	body := data[:len(data)-4]
	wantCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != wantCRC || !bytes.Equal(body[:len(sqlConnectorTransactionRetryJournalMagic)], sqlConnectorTransactionRetryJournalMagic) {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if body[len(sqlConnectorTransactionRetryJournalMagic)] != sqlConnectorTransactionRetryJournalVersion {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	reader := sqlConnectorTransactionRetryJournalReader{data: body, offset: len(sqlConnectorTransactionRetryJournalMagic) + 1}
	capacity, ok := reader.uvarint()
	if !ok || capacity == 0 || capacity > MaxSQLConnectorTransactionRetryJournalCapacity {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	dropped, ok := reader.uvarint()
	if !ok {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	nextSequence, ok := reader.uvarint()
	if !ok || nextSequence == 0 {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	recordCount, ok := reader.uvarint()
	if !ok || recordCount > capacity || recordCount > MaxSQLConnectorTransactionRetryJournalCapacity {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	snapshot := SQLConnectorTransactionRetryJournalSnapshot{
		Capacity:     int(capacity),
		Dropped:      dropped,
		NextSequence: nextSequence,
		Records:      make([]SQLConnectorTransactionRecord, 0, int(recordCount)),
	}
	for index := uint64(0); index < recordCount; index++ {
		sequence, ok := reader.uvarint()
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		source, ok := reader.string(MaxSQLConnectorTransactionRetryJournalStringBytes)
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		transactionID, ok := reader.string(MaxSQLConnectorTransactionRetryJournalStringBytes)
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		stateCode, ok := reader.byte()
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		state, ok := sqlConnectorTransactionRetryJournalStateFromCode(stateCode)
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		attempts, ok := reader.uvarint()
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		hasTime, ok := reader.byte()
		if !ok || hasTime > 1 {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		var updatedAt time.Time
		if hasTime == 1 {
			seconds, ok := reader.varint()
			if !ok {
				return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			nanoseconds, ok := reader.uvarint()
			if !ok || nanoseconds >= uint64(time.Second) {
				return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			updatedAt = time.Unix(seconds, int64(nanoseconds)).UTC().Round(0)
		}
		errorCode, ok := reader.string(MaxSQLConnectorTransactionRetryJournalErrorCodeBytes)
		if !ok {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		relationCount, ok := reader.uvarint()
		if !ok || relationCount > DefaultSQLSourceTransactionEnvelopeMaxRelations {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		relations := make([]string, 0, int(relationCount))
		for relationIndex := uint64(0); relationIndex < relationCount; relationIndex++ {
			relation, ok := reader.string(MaxSQLConnectorTransactionRetryJournalStringBytes)
			if !ok {
				return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			relations = append(relations, relation)
		}
		offsetCount, ok := reader.uvarint()
		if !ok || offsetCount == 0 || offsetCount > MaxSQLConnectorTransactionRetryJournalOffsets {
			return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		offsets := make([]SQLSourceOffset, 0, int(offsetCount))
		for offsetIndex := uint64(0); offsetIndex < offsetCount; offsetIndex++ {
			partition, ok := reader.string(MaxSQLConnectorTransactionRetryJournalStringBytes)
			if !ok {
				return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			offset, ok := reader.uvarint()
			if !ok {
				return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
			}
			offsets = append(offsets, SQLSourceOffset{Source: source, Partition: partition, Offset: offset})
		}
		snapshot.Records = append(snapshot.Records, SQLConnectorTransactionRecord{
			Sequence: sequence,
			Envelope: SQLSourceTransactionEnvelope{
				Source:      source,
				Transaction: SQLSourceTransaction{ID: transactionID, Offsets: offsets},
				Relations:   relations,
			},
			State: state, Attempts: attempts, LastErrorCode: errorCode, UpdatedAt: updatedAt,
		})
	}
	if reader.offset != len(body) {
		return SQLConnectorTransactionRetryJournalSnapshot{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	_, _, _, err := validateSQLConnectorTransactionRetryJournalSnapshot(snapshot)
	if err != nil {
		return SQLConnectorTransactionRetryJournalSnapshot{}, err
	}
	return snapshot, nil
}

func (j *SQLConnectorTransactionRetryJournal) evictOldestTerminalLocked() bool {
	for index, key := range j.order {
		record, found := j.records[key]
		if !found {
			continue
		}
		if record.State != SQLConnectorTransactionCommitted && record.State != SQLConnectorTransactionAborted {
			continue
		}
		delete(j.records, key)
		copy(j.order[index:], j.order[index+1:])
		j.order = j.order[:len(j.order)-1]
		j.dropped++
		return true
	}
	return false
}

func normalizeSQLConnectorTransactionRetryJournalCapacity(capacity int) (int, error) {
	if capacity == 0 {
		capacity = DefaultSQLConnectorTransactionRetryJournalCapacity
	}
	if capacity < 1 || capacity > MaxSQLConnectorTransactionRetryJournalCapacity {
		return 0, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	return capacity, nil
}

func normalizeSQLConnectorTransactionRetryJournalEnvelope(envelope SQLSourceTransactionEnvelope) (sqlConnectorTransactionRetryJournalKey, SQLSourceTransactionEnvelope, error) {
	if validateSQLConnectorTransactionRetryJournalEnvelopeInput(envelope) != nil {
		return sqlConnectorTransactionRetryJournalKey{}, SQLSourceTransactionEnvelope{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	key, normalized, err := normalizeSQLSourceTransactionEnvelope(envelope, false)
	if err != nil || validateSQLConnectorTransactionRetryJournalEnvelopeInput(normalized) != nil {
		return sqlConnectorTransactionRetryJournalKey{}, SQLSourceTransactionEnvelope{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	return sqlConnectorTransactionRetryJournalKey{source: key.source, transactionID: key.transactionID}, normalized, nil
}

func validateSQLConnectorTransactionRetryJournalEnvelopeInput(envelope SQLSourceTransactionEnvelope) error {
	if validateSQLConnectorTransactionRetryJournalText(envelope.Source, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil || validateSQLConnectorTransactionRetryJournalText(envelope.Transaction.ID, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if len(envelope.Transaction.Offsets) == 0 || len(envelope.Transaction.Offsets) > MaxSQLConnectorTransactionRetryJournalOffsets {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	for _, offset := range envelope.Transaction.Offsets {
		if validateSQLConnectorTransactionRetryJournalText(offset.Source, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil || validateSQLConnectorTransactionRetryJournalText(offset.Partition, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil {
			return ErrSQLConnectorTransactionRetryJournalInvalid
		}
	}
	if len(envelope.Relations) > DefaultSQLSourceTransactionEnvelopeMaxRelations {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	for _, relation := range envelope.Relations {
		if validateSQLConnectorTransactionRetryJournalText(relation, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil {
			return ErrSQLConnectorTransactionRetryJournalInvalid
		}
	}
	return nil
}

func normalizeSQLConnectorTransactionRetryJournalCompletion(completion SQLConnectorTransactionCompletion) (sqlConnectorTransactionRetryJournalKey, SQLConnectorTransactionCompletion, error) {
	if validateSQLConnectorTransactionRetryJournalText(completion.Source, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil || validateSQLConnectorTransactionRetryJournalText(completion.TransactionID, MaxSQLConnectorTransactionRetryJournalStringBytes, true) != nil || completion.Attempt == 0 {
		return sqlConnectorTransactionRetryJournalKey{}, SQLConnectorTransactionCompletion{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if completion.Outcome != SQLConnectorTransactionRetryable && completion.Outcome != SQLConnectorTransactionCommitted && completion.Outcome != SQLConnectorTransactionAborted {
		return sqlConnectorTransactionRetryJournalKey{}, SQLConnectorTransactionCompletion{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if validateSQLConnectorTransactionRetryJournalErrorCode(completion.ErrorCode) != nil {
		return sqlConnectorTransactionRetryJournalKey{}, SQLConnectorTransactionCompletion{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if (completion.Outcome == SQLConnectorTransactionRetryable || completion.Outcome == SQLConnectorTransactionAborted) && completion.ErrorCode == "" {
		return sqlConnectorTransactionRetryJournalKey{}, SQLConnectorTransactionCompletion{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	if completion.Outcome == SQLConnectorTransactionCommitted && completion.ErrorCode != "" {
		return sqlConnectorTransactionRetryJournalKey{}, SQLConnectorTransactionCompletion{}, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	completion.Source = strings.TrimSpace(completion.Source)
	completion.TransactionID = strings.TrimSpace(completion.TransactionID)
	return sqlConnectorTransactionRetryJournalKey{source: completion.Source, transactionID: completion.TransactionID}, completion, nil
}

func validateSQLConnectorTransactionRetryJournalErrorCode(code string) error {
	if len(code) > MaxSQLConnectorTransactionRetryJournalErrorCodeBytes {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	for index := 0; index < len(code); index++ {
		character := code[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' {
			continue
		}
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	return nil
}

func validateSQLConnectorTransactionRetryJournalText(value string, maximum int, required bool) error {
	if !utf8.ValidString(value) || len(value) > maximum {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return ErrSQLConnectorTransactionRetryJournalInvalid
		}
	}
	if required && strings.TrimSpace(value) == "" {
		return ErrSQLConnectorTransactionRetryJournalInvalid
	}
	return nil
}

func cloneSQLConnectorTransactionRetryJournalRecord(record SQLConnectorTransactionRecord) SQLConnectorTransactionRecord {
	record.Envelope.Transaction.Offsets = append([]SQLSourceOffset(nil), record.Envelope.Transaction.Offsets...)
	record.Envelope.Relations = append([]string(nil), record.Envelope.Relations...)
	return record
}

func sqlConnectorTransactionRetryJournalNow() time.Time {
	return time.Now().UTC().Round(0)
}

func validateSQLConnectorTransactionRetryJournalSnapshot(snapshot SQLConnectorTransactionRetryJournalSnapshot) (map[sqlConnectorTransactionRetryJournalKey]SQLConnectorTransactionRecord, []sqlConnectorTransactionRetryJournalKey, int, error) {
	capacity, err := normalizeSQLConnectorTransactionRetryJournalCapacity(snapshot.Capacity)
	if err != nil || snapshot.NextSequence == 0 || len(snapshot.Records) > capacity {
		return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	replacement := make(map[sqlConnectorTransactionRetryJournalKey]SQLConnectorTransactionRecord, len(snapshot.Records))
	order := make([]sqlConnectorTransactionRetryJournalKey, 0, len(snapshot.Records))
	var previousSequence uint64
	for index, record := range snapshot.Records {
		key, normalized, err := normalizeSQLConnectorTransactionRetryJournalEnvelope(record.Envelope)
		if err != nil || record.Sequence == 0 || record.Attempts == 0 || (index > 0 && record.Sequence <= previousSequence) {
			return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		if !sqlConnectorTransactionRetryJournalValidState(record.State) || validateSQLConnectorTransactionRetryJournalErrorCode(record.LastErrorCode) != nil {
			return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		if (record.State == SQLConnectorTransactionRetryable || record.State == SQLConnectorTransactionAborted) && record.LastErrorCode == "" {
			return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		if (record.State == SQLConnectorTransactionPending || record.State == SQLConnectorTransactionCommitted) && record.LastErrorCode != "" {
			return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		if !record.UpdatedAt.IsZero() {
			record.UpdatedAt = record.UpdatedAt.UTC().Round(0)
		}
		record.Envelope = normalized
		if _, found := replacement[key]; found {
			return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
		}
		replacement[key] = cloneSQLConnectorTransactionRetryJournalRecord(record)
		order = append(order, key)
		previousSequence = record.Sequence
	}
	if snapshot.NextSequence <= previousSequence {
		return nil, nil, 0, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	return replacement, order, capacity, nil
}

func sqlConnectorTransactionRetryJournalValidState(state SQLConnectorTransactionRetryJournalState) bool {
	return state == SQLConnectorTransactionPending || state == SQLConnectorTransactionRetryable || state == SQLConnectorTransactionCommitted || state == SQLConnectorTransactionAborted
}

var (
	sqlConnectorTransactionRetryJournalMagic   = []byte{'H', 'T', 'J', '1'}
	sqlConnectorTransactionRetryJournalVersion = byte(1)
)

func marshalSQLConnectorTransactionRetryJournalSnapshot(snapshot SQLConnectorTransactionRetryJournalSnapshot) ([]byte, error) {
	_, _, capacity, err := validateSQLConnectorTransactionRetryJournalSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Records) > capacity {
		return nil, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	body := make([]byte, 0, 128+len(snapshot.Records)*96)
	body = append(body, sqlConnectorTransactionRetryJournalMagic...)
	body = append(body, sqlConnectorTransactionRetryJournalVersion)
	body = appendSQLConnectorTransactionRetryJournalUvarint(body, uint64(capacity))
	body = appendSQLConnectorTransactionRetryJournalUvarint(body, snapshot.Dropped)
	body = appendSQLConnectorTransactionRetryJournalUvarint(body, snapshot.NextSequence)
	body = appendSQLConnectorTransactionRetryJournalUvarint(body, uint64(len(snapshot.Records)))
	for _, record := range snapshot.Records {
		body = appendSQLConnectorTransactionRetryJournalRecord(body, record)
	}
	if len(body)+4 > MaxSQLConnectorTransactionRetryJournalSnapshotBytes {
		return nil, ErrSQLConnectorTransactionRetryJournalInvalid
	}
	checksum := crc32.ChecksumIEEE(body)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	body = append(body, checksumBytes[:]...)
	return body, nil
}

func appendSQLConnectorTransactionRetryJournalRecord(data []byte, record SQLConnectorTransactionRecord) []byte {
	data = appendSQLConnectorTransactionRetryJournalUvarint(data, record.Sequence)
	data = appendSQLConnectorTransactionRetryJournalString(data, record.Envelope.Source)
	data = appendSQLConnectorTransactionRetryJournalString(data, record.Envelope.Transaction.ID)
	stateCode, _ := sqlConnectorTransactionRetryJournalStateCode(record.State)
	data = append(data, stateCode)
	data = appendSQLConnectorTransactionRetryJournalUvarint(data, record.Attempts)
	if record.UpdatedAt.IsZero() {
		data = append(data, 0)
	} else {
		data = append(data, 1)
		data = appendSQLConnectorTransactionRetryJournalVarint(data, record.UpdatedAt.Unix())
		data = appendSQLConnectorTransactionRetryJournalUvarint(data, uint64(record.UpdatedAt.Nanosecond()))
	}
	data = appendSQLConnectorTransactionRetryJournalString(data, record.LastErrorCode)
	data = appendSQLConnectorTransactionRetryJournalUvarint(data, uint64(len(record.Envelope.Relations)))
	for _, relation := range record.Envelope.Relations {
		data = appendSQLConnectorTransactionRetryJournalString(data, relation)
	}
	data = appendSQLConnectorTransactionRetryJournalUvarint(data, uint64(len(record.Envelope.Transaction.Offsets)))
	for _, offset := range record.Envelope.Transaction.Offsets {
		data = appendSQLConnectorTransactionRetryJournalString(data, offset.Partition)
		data = appendSQLConnectorTransactionRetryJournalUvarint(data, offset.Offset)
	}
	return data
}

func sqlConnectorTransactionRetryJournalStateCode(state SQLConnectorTransactionRetryJournalState) (byte, bool) {
	switch state {
	case SQLConnectorTransactionPending:
		return 0, true
	case SQLConnectorTransactionRetryable:
		return 1, true
	case SQLConnectorTransactionCommitted:
		return 2, true
	case SQLConnectorTransactionAborted:
		return 3, true
	default:
		return 0, false
	}
}

func sqlConnectorTransactionRetryJournalStateFromCode(code byte) (SQLConnectorTransactionRetryJournalState, bool) {
	switch code {
	case 0:
		return SQLConnectorTransactionPending, true
	case 1:
		return SQLConnectorTransactionRetryable, true
	case 2:
		return SQLConnectorTransactionCommitted, true
	case 3:
		return SQLConnectorTransactionAborted, true
	default:
		return "", false
	}
}

func appendSQLConnectorTransactionRetryJournalUvarint(data []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(data, encoded[:length]...)
}

func appendSQLConnectorTransactionRetryJournalVarint(data []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutVarint(encoded[:], value)
	return append(data, encoded[:length]...)
}

func appendSQLConnectorTransactionRetryJournalString(data []byte, value string) []byte {
	data = appendSQLConnectorTransactionRetryJournalUvarint(data, uint64(len(value)))
	return append(data, value...)
}

type sqlConnectorTransactionRetryJournalReader struct {
	data   []byte
	offset int
}

func (reader *sqlConnectorTransactionRetryJournalReader) byte() (byte, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, true
}

func (reader *sqlConnectorTransactionRetryJournalReader) uvarint() (uint64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, length := binary.Uvarint(reader.data[reader.offset:])
	if length <= 0 || length > binary.MaxVarintLen64 {
		return 0, false
	}
	reader.offset += length
	return value, true
}

func (reader *sqlConnectorTransactionRetryJournalReader) varint() (int64, bool) {
	if reader.offset >= len(reader.data) {
		return 0, false
	}
	value, length := binary.Varint(reader.data[reader.offset:])
	if length <= 0 || length > binary.MaxVarintLen64 {
		return 0, false
	}
	reader.offset += length
	return value, true
}

func (reader *sqlConnectorTransactionRetryJournalReader) string(maximum int) (string, bool) {
	length, ok := reader.uvarint()
	if !ok || length > uint64(maximum) || length > uint64(len(reader.data)-reader.offset) {
		return "", false
	}
	start := reader.offset
	reader.offset += int(length)
	return string(reader.data[start:reader.offset]), true
}
