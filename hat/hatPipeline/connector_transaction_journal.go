package hatPipeline

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
)

var (
	ErrConnectorTransactionJournalNil         = errors.New("hatPipeline: connector transaction journal is nil")
	ErrConnectorTransactionJournalOptions     = errors.New("hatPipeline: connector transaction journal options are invalid")
	ErrConnectorTransactionInvalid            = errors.New("hatPipeline: connector transaction is invalid")
	ErrConnectorTransactionCorrupt            = errors.New("hatPipeline: connector transaction snapshot is corrupt")
	ErrConnectorTransactionStoreRequired      = errors.New("hatPipeline: connector transaction journal store is required")
	ErrConnectorTransactionNotFound           = errors.New("hatPipeline: connector transaction was not found")
	ErrConnectorTransactionIntentMismatch     = errors.New("hatPipeline: connector transaction intent does not match")
	ErrConnectorTransactionGenerationMismatch = errors.New("hatPipeline: connector transaction generation does not match")
	ErrConnectorTransactionStateInvalid       = errors.New("hatPipeline: connector transaction state is invalid")
	ErrConnectorTransactionJournalFull        = errors.New("hatPipeline: connector transaction journal has no evictable record")
	ErrConnectorTransactionAttemptLimit       = errors.New("hatPipeline: connector transaction retry limit reached")
	ErrConnectorTransactionPayloadTooLarge    = errors.New("hatPipeline: connector transaction snapshot is too large")
)

const (
	DefaultConnectorTransactionJournalCapacity = 256
	MaxConnectorTransactionJournalCapacity     = 4096
	MaxConnectorTransactionIDBytes             = 256
	MaxConnectorTransactionConnectorIDBytes    = 256
	MaxConnectorTransactionOffsetBytes         = 512 << 10
	MaxConnectorTransactionFrontierBytes       = 512 << 10
	MaxConnectorTransactionErrorBytes          = 4096
	MaxConnectorTransactionAttempts            = 1 << 20
	MaxConnectorTransactionSnapshotBytes       = 1 << 20
	connectorTransactionFormatVersion          = 1
	connectorTransactionMagic                  = "HCT1"
	connectorTransactionHeaderBytes            = 4 + 2 + 8 + 8 + 4
	connectorTransactionChecksumBytes          = 4
	connectorTransactionFixedRecordBytes       = 8 + 8 + 8 + 4 + 1 + 2 + 2 + 4 + 4 + 4
)

var connectorTransactionCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ConnectorTransactionState describes the durable outcome of one source
// transaction intent.
type ConnectorTransactionState uint8

const (
	ConnectorTransactionInvalid ConnectorTransactionState = iota
	ConnectorTransactionPending
	ConnectorTransactionFailed
	ConnectorTransactionCommitted
	ConnectorTransactionAborted
)

func (state ConnectorTransactionState) String() string {
	switch state {
	case ConnectorTransactionPending:
		return "pending"
	case ConnectorTransactionFailed:
		return "failed"
	case ConnectorTransactionCommitted:
		return "committed"
	case ConnectorTransactionAborted:
		return "aborted"
	default:
		return "invalid"
	}
}

// ConnectorTransactionIntent is the caller-owned identity and source
// position for one idempotent transaction attempt.
type ConnectorTransactionIntent struct {
	ID          string
	ConnectorID string
	Generation  uint64
	Offset      []byte
	Frontier    []byte
}

// ConnectorTransaction is the durable state of one transaction ID. Sequence
// changes on every journal mutation; CreatedSequence remains stable for
// bounded retention and deterministic recovery order.
type ConnectorTransaction struct {
	ID              string
	ConnectorID     string
	Generation      uint64
	Sequence        uint64
	CreatedSequence uint64
	Attempt         uint32
	State           ConnectorTransactionState
	Offset          []byte
	Frontier        []byte
	LastError       string
}

// ConnectorTransactionSnapshot is the complete bounded journal image.
type ConnectorTransactionSnapshot struct {
	Revision     uint64
	Dropped      uint64
	Transactions []ConnectorTransaction
}

// ConnectorTransactionJournalStore persists a complete HCT1 image. The
// existing checkpoint file stores implement this shape and can be reused.
type ConnectorTransactionJournalStore interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
}

// ConnectorTransactionJournalOptions configures retention and optional
// durable storage. A nil Store makes the journal process-local.
type ConnectorTransactionJournalOptions struct {
	Capacity int
	Store    ConnectorTransactionJournalStore
}

// ConnectorTransactionJournal is a bounded, concurrency-safe transaction
// intent/outcome journal. It never retries source work itself; Retry advances
// a durable state machine so a caller can safely perform the next attempt.
type ConnectorTransactionJournal struct {
	mu           sync.RWMutex
	capacity     int
	store        ConnectorTransactionJournalStore
	revision     uint64
	dropped      uint64
	transactions []ConnectorTransaction
}

// NewConnectorTransactionJournal creates a journal and restores its optional
// store. A missing checkpoint-style store is treated as an empty journal.
func NewConnectorTransactionJournal(ctx context.Context, options ConnectorTransactionJournalOptions) (*ConnectorTransactionJournal, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConnectorTransactionJournalCapacity
	}
	if capacity < 1 || capacity > MaxConnectorTransactionJournalCapacity {
		return nil, ErrConnectorTransactionJournalOptions
	}
	journal := &ConnectorTransactionJournal{
		capacity:     capacity,
		store:        options.Store,
		transactions: make([]ConnectorTransaction, 0, capacity),
	}
	if options.Store == nil {
		return journal, nil
	}
	payload, err := options.Store.Load(ctx)
	if err != nil {
		if errors.Is(err, ErrConnectorCheckpointNotFound) {
			return journal, nil
		}
		return nil, err
	}
	if len(payload) == 0 {
		return journal, nil
	}
	snapshot, err := DecodeConnectorTransactionSnapshot(payload)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Transactions) > capacity {
		trim := len(snapshot.Transactions) - capacity
		snapshot.Transactions = snapshot.Transactions[trim:]
		snapshot.Dropped += uint64(trim)
	}
	journal.revision = snapshot.Revision
	journal.dropped = snapshot.Dropped
	journal.transactions = make([]ConnectorTransaction, len(snapshot.Transactions), capacity)
	for index, transaction := range snapshot.Transactions {
		journal.transactions[index] = cloneConnectorTransaction(transaction)
	}
	return journal, nil
}

// Begin records a new pending intent. Repeating the exact same intent is
// idempotent and returns the original record without a new journal revision.
func (journal *ConnectorTransactionJournal) Begin(ctx context.Context, intent ConnectorTransactionIntent) (ConnectorTransaction, error) {
	if err := validateConnectorTransactionContext(journal, ctx); err != nil {
		return ConnectorTransaction{}, err
	}
	if err := validateConnectorTransactionIntent(intent); err != nil {
		return ConnectorTransaction{}, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if index := journal.findLocked(intent.ID); index >= 0 {
		existing := journal.transactions[index]
		if connectorTransactionMatchesIntent(existing, intent) {
			return cloneConnectorTransaction(existing), nil
		}
		return ConnectorTransaction{}, ErrConnectorTransactionIntentMismatch
	}
	var previous ConnectorTransactionSnapshot
	if journal.store != nil {
		previous = journal.captureLocked()
	}
	if len(journal.transactions) >= journal.capacity {
		evictIndex := journal.firstTerminalLocked()
		if evictIndex < 0 {
			return ConnectorTransaction{}, ErrConnectorTransactionJournalFull
		}
		copy(journal.transactions[evictIndex:], journal.transactions[evictIndex+1:])
		journal.transactions = journal.transactions[:len(journal.transactions)-1]
		journal.dropped++
	}
	if journal.revision == ^uint64(0) {
		return ConnectorTransaction{}, ErrConnectorTransactionJournalOptions
	}
	journal.revision++
	transaction := ConnectorTransaction{
		ID:              intent.ID,
		ConnectorID:     intent.ConnectorID,
		Generation:      intent.Generation,
		Sequence:        journal.revision,
		CreatedSequence: journal.revision,
		Attempt:         1,
		State:           ConnectorTransactionPending,
		Offset:          append([]byte(nil), intent.Offset...),
		Frontier:        append([]byte(nil), intent.Frontier...),
	}
	journal.transactions = append(journal.transactions, transaction)
	if err := journal.persistLocked(ctx); err != nil {
		journal.restoreLocked(previous)
		return ConnectorTransaction{}, err
	}
	return cloneConnectorTransaction(transaction), nil
}

// Fail records a retryable source failure. Repeating failure for an already
// failed transaction is idempotent and returns its existing outcome.
func (journal *ConnectorTransactionJournal) Fail(ctx context.Context, id string, generation uint64, failure string) (ConnectorTransaction, error) {
	if err := validateConnectorTransactionContext(journal, ctx); err != nil {
		return ConnectorTransaction{}, err
	}
	if err := validateConnectorTransactionID(id); err != nil || generation == 0 || len(failure) > MaxConnectorTransactionErrorBytes || strings.IndexByte(failure, 0) >= 0 {
		return ConnectorTransaction{}, ErrConnectorTransactionInvalid
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	index, err := journal.lookupLocked(id, generation)
	if err != nil {
		return ConnectorTransaction{}, err
	}
	current := journal.transactions[index]
	if current.State == ConnectorTransactionFailed {
		return cloneConnectorTransaction(current), nil
	}
	if current.State != ConnectorTransactionPending {
		return ConnectorTransaction{}, ErrConnectorTransactionStateInvalid
	}
	var previous ConnectorTransactionSnapshot
	if journal.store != nil {
		previous = journal.captureLocked()
	}
	if err := journal.advanceLocked(&current); err != nil {
		return ConnectorTransaction{}, err
	}
	current.State = ConnectorTransactionFailed
	current.LastError = failure
	journal.transactions[index] = current
	if err := journal.persistLocked(ctx); err != nil {
		journal.restoreLocked(previous)
		return ConnectorTransaction{}, err
	}
	return cloneConnectorTransaction(current), nil
}

// Retry turns a failed transaction back into a pending attempt. It preserves
// the same transaction ID, connector generation, and source positions.
func (journal *ConnectorTransactionJournal) Retry(ctx context.Context, id string, generation uint64) (ConnectorTransaction, error) {
	if err := validateConnectorTransactionContext(journal, ctx); err != nil {
		return ConnectorTransaction{}, err
	}
	if err := validateConnectorTransactionID(id); err != nil || generation == 0 {
		return ConnectorTransaction{}, ErrConnectorTransactionInvalid
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	index, err := journal.lookupLocked(id, generation)
	if err != nil {
		return ConnectorTransaction{}, err
	}
	current := journal.transactions[index]
	if current.State == ConnectorTransactionPending {
		return cloneConnectorTransaction(current), nil
	}
	if current.State != ConnectorTransactionFailed {
		return ConnectorTransaction{}, ErrConnectorTransactionStateInvalid
	}
	if current.Attempt >= MaxConnectorTransactionAttempts {
		return ConnectorTransaction{}, ErrConnectorTransactionAttemptLimit
	}
	var previous ConnectorTransactionSnapshot
	if journal.store != nil {
		previous = journal.captureLocked()
	}
	if err := journal.advanceLocked(&current); err != nil {
		return ConnectorTransaction{}, err
	}
	current.Attempt++
	current.State = ConnectorTransactionPending
	current.LastError = ""
	journal.transactions[index] = current
	if err := journal.persistLocked(ctx); err != nil {
		journal.restoreLocked(previous)
		return ConnectorTransaction{}, err
	}
	return cloneConnectorTransaction(current), nil
}

// Commit records a successful delivery. Repeating commit is idempotent.
func (journal *ConnectorTransactionJournal) Commit(ctx context.Context, id string, generation uint64) (ConnectorTransaction, error) {
	return journal.finish(ctx, id, generation, ConnectorTransactionCommitted, "")
}

// Abort records a terminal non-retry outcome.
func (journal *ConnectorTransactionJournal) Abort(ctx context.Context, id string, generation uint64, reason string) (ConnectorTransaction, error) {
	if len(reason) > MaxConnectorTransactionErrorBytes || strings.IndexByte(reason, 0) >= 0 {
		return ConnectorTransaction{}, ErrConnectorTransactionInvalid
	}
	return journal.finish(ctx, id, generation, ConnectorTransactionAborted, reason)
}

func (journal *ConnectorTransactionJournal) finish(ctx context.Context, id string, generation uint64, state ConnectorTransactionState, reason string) (ConnectorTransaction, error) {
	if err := validateConnectorTransactionContext(journal, ctx); err != nil {
		return ConnectorTransaction{}, err
	}
	if err := validateConnectorTransactionID(id); err != nil || generation == 0 {
		return ConnectorTransaction{}, ErrConnectorTransactionInvalid
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	index, err := journal.lookupLocked(id, generation)
	if err != nil {
		return ConnectorTransaction{}, err
	}
	current := journal.transactions[index]
	if current.State == state {
		return cloneConnectorTransaction(current), nil
	}
	if current.State != ConnectorTransactionPending && !(state == ConnectorTransactionAborted && current.State == ConnectorTransactionFailed) {
		return ConnectorTransaction{}, ErrConnectorTransactionStateInvalid
	}
	var previous ConnectorTransactionSnapshot
	if journal.store != nil {
		previous = journal.captureLocked()
	}
	if err := journal.advanceLocked(&current); err != nil {
		return ConnectorTransaction{}, err
	}
	current.State = state
	current.LastError = reason
	journal.transactions[index] = current
	if err := journal.persistLocked(ctx); err != nil {
		journal.restoreLocked(previous)
		return ConnectorTransaction{}, err
	}
	return cloneConnectorTransaction(current), nil
}

// Get returns an independent transaction record.
func (journal *ConnectorTransactionJournal) Get(id string) (ConnectorTransaction, bool) {
	if journal == nil || id == "" {
		return ConnectorTransaction{}, false
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	index := journal.findLocked(id)
	if index < 0 {
		return ConnectorTransaction{}, false
	}
	return cloneConnectorTransaction(journal.transactions[index]), true
}

// Snapshot returns an independent, deterministic copy of retained records.
func (journal *ConnectorTransactionJournal) Snapshot() ConnectorTransactionSnapshot {
	if journal == nil {
		return ConnectorTransactionSnapshot{}
	}
	journal.mu.RLock()
	snapshot := journal.captureLocked()
	journal.mu.RUnlock()
	sort.Slice(snapshot.Transactions, func(i, j int) bool {
		left, right := snapshot.Transactions[i], snapshot.Transactions[j]
		if left.CreatedSequence != right.CreatedSequence {
			return left.CreatedSequence < right.CreatedSequence
		}
		return left.ID < right.ID
	})
	return snapshot
}

// EncodeConnectorTransactionSnapshot serializes a bounded HCT1 snapshot with
// deterministic ordering and a CRC32C trailer.
func EncodeConnectorTransactionSnapshot(snapshot ConnectorTransactionSnapshot) ([]byte, error) {
	if err := validateConnectorTransactionSnapshot(snapshot); err != nil {
		return nil, err
	}
	transactions := append([]ConnectorTransaction(nil), snapshot.Transactions...)
	sort.Slice(transactions, func(i, j int) bool {
		if transactions[i].CreatedSequence != transactions[j].CreatedSequence {
			return transactions[i].CreatedSequence < transactions[j].CreatedSequence
		}
		return transactions[i].ID < transactions[j].ID
	})
	total := connectorTransactionHeaderBytes + connectorTransactionChecksumBytes
	for _, transaction := range transactions {
		total += connectorTransactionFixedRecordBytes + len(transaction.ID) + len(transaction.ConnectorID) + len(transaction.Offset) + len(transaction.Frontier) + len(transaction.LastError)
	}
	if total > MaxConnectorTransactionSnapshotBytes {
		return nil, ErrConnectorTransactionPayloadTooLarge
	}
	payload := make([]byte, 0, total)
	payload = append(payload, connectorTransactionMagic...)
	var numeric [8]byte
	var small [4]byte
	binary.BigEndian.PutUint16(small[:2], connectorTransactionFormatVersion)
	payload = append(payload, small[:2]...)
	binary.BigEndian.PutUint64(numeric[:], snapshot.Revision)
	payload = append(payload, numeric[:]...)
	binary.BigEndian.PutUint64(numeric[:], snapshot.Dropped)
	payload = append(payload, numeric[:]...)
	binary.BigEndian.PutUint32(small[:], uint32(len(transactions)))
	payload = append(payload, small[:]...)
	for _, transaction := range transactions {
		var fixed [connectorTransactionFixedRecordBytes]byte
		binary.BigEndian.PutUint64(fixed[0:8], transaction.Sequence)
		binary.BigEndian.PutUint64(fixed[8:16], transaction.CreatedSequence)
		binary.BigEndian.PutUint64(fixed[16:24], transaction.Generation)
		binary.BigEndian.PutUint32(fixed[24:28], transaction.Attempt)
		fixed[28] = byte(transaction.State)
		binary.BigEndian.PutUint16(fixed[29:31], uint16(len(transaction.ID)))
		binary.BigEndian.PutUint16(fixed[31:33], uint16(len(transaction.ConnectorID)))
		binary.BigEndian.PutUint32(fixed[33:37], uint32(len(transaction.Offset)))
		binary.BigEndian.PutUint32(fixed[37:41], uint32(len(transaction.Frontier)))
		binary.BigEndian.PutUint32(fixed[41:45], uint32(len(transaction.LastError)))
		payload = append(payload, fixed[:]...)
		payload = append(payload, transaction.ID...)
		payload = append(payload, transaction.ConnectorID...)
		payload = append(payload, transaction.Offset...)
		payload = append(payload, transaction.Frontier...)
		payload = append(payload, transaction.LastError...)
	}
	binary.BigEndian.PutUint32(small[:], crc32.Checksum(payload, connectorTransactionCRCTable))
	payload = append(payload, small[:]...)
	return payload, nil
}

// DecodeConnectorTransactionSnapshot validates one HCT1 snapshot before
// allocating any field buffers from its untrusted lengths.
func DecodeConnectorTransactionSnapshot(payload []byte) (ConnectorTransactionSnapshot, error) {
	minimum := connectorTransactionHeaderBytes + connectorTransactionChecksumBytes
	if len(payload) < minimum || len(payload) > MaxConnectorTransactionSnapshotBytes || string(payload[:4]) != connectorTransactionMagic {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	if binary.BigEndian.Uint16(payload[4:6]) != connectorTransactionFormatVersion {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	bodyEnd := len(payload) - connectorTransactionChecksumBytes
	if crc32.Checksum(payload[:bodyEnd], connectorTransactionCRCTable) != binary.BigEndian.Uint32(payload[bodyEnd:]) {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	snapshot := ConnectorTransactionSnapshot{
		Revision: binary.BigEndian.Uint64(payload[6:14]),
		Dropped:  binary.BigEndian.Uint64(payload[14:22]),
	}
	count := binary.BigEndian.Uint32(payload[22:26])
	if count > MaxConnectorTransactionJournalCapacity {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	if uint64(count)*connectorTransactionFixedRecordBytes > uint64(bodyEnd-connectorTransactionHeaderBytes) {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	position := connectorTransactionHeaderBytes
	snapshot.Transactions = make([]ConnectorTransaction, 0, count)
	for index := uint32(0); index < count; index++ {
		if position+connectorTransactionFixedRecordBytes > bodyEnd {
			return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
		}
		fixed := payload[position : position+connectorTransactionFixedRecordBytes]
		position += connectorTransactionFixedRecordBytes
		idLength := int(binary.BigEndian.Uint16(fixed[29:31]))
		connectorLength := int(binary.BigEndian.Uint16(fixed[31:33]))
		offsetLength := uint64(binary.BigEndian.Uint32(fixed[33:37]))
		frontierLength := uint64(binary.BigEndian.Uint32(fixed[37:41]))
		errorLength := uint64(binary.BigEndian.Uint32(fixed[41:45]))
		if idLength == 0 || idLength > MaxConnectorTransactionIDBytes || connectorLength == 0 || connectorLength > MaxConnectorTransactionConnectorIDBytes || offsetLength > MaxConnectorTransactionOffsetBytes || frontierLength > MaxConnectorTransactionFrontierBytes || errorLength > MaxConnectorTransactionErrorBytes {
			return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
		}
		total := uint64(idLength+connectorLength) + offsetLength + frontierLength + errorLength
		if total > uint64(bodyEnd-position) {
			return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
		}
		transaction := ConnectorTransaction{
			Sequence:        binary.BigEndian.Uint64(fixed[0:8]),
			CreatedSequence: binary.BigEndian.Uint64(fixed[8:16]),
			Generation:      binary.BigEndian.Uint64(fixed[16:24]),
			Attempt:         binary.BigEndian.Uint32(fixed[24:28]),
			State:           ConnectorTransactionState(fixed[28]),
		}
		transaction.ID = string(payload[position : position+idLength])
		position += idLength
		transaction.ConnectorID = string(payload[position : position+connectorLength])
		position += connectorLength
		transaction.Offset = append([]byte(nil), payload[position:position+int(offsetLength)]...)
		position += int(offsetLength)
		transaction.Frontier = append([]byte(nil), payload[position:position+int(frontierLength)]...)
		position += int(frontierLength)
		transaction.LastError = string(payload[position : position+int(errorLength)])
		position += int(errorLength)
		if err := validateConnectorTransaction(transaction); err != nil {
			return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
		}
		snapshot.Transactions = append(snapshot.Transactions, transaction)
	}
	if position != bodyEnd {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	if err := validateConnectorTransactionSnapshot(snapshot); err != nil {
		return ConnectorTransactionSnapshot{}, ErrConnectorTransactionCorrupt
	}
	return snapshot, nil
}

func validateConnectorTransactionContext(journal *ConnectorTransactionJournal, ctx context.Context) error {
	if journal == nil {
		return ErrConnectorTransactionJournalNil
	}
	if ctx == nil {
		return ErrConnectorTransactionInvalid
	}
	return ctx.Err()
}

func validateConnectorTransactionIntent(intent ConnectorTransactionIntent) error {
	if validateConnectorTransactionID(intent.ID) != nil || validateConnectorTransactionConnectorID(intent.ConnectorID) != nil || intent.Generation == 0 || len(intent.Offset) > MaxConnectorTransactionOffsetBytes || len(intent.Frontier) > MaxConnectorTransactionFrontierBytes {
		return ErrConnectorTransactionInvalid
	}
	return nil
}

func validateConnectorTransactionID(id string) error {
	if id == "" || strings.TrimSpace(id) != id || len(id) > MaxConnectorTransactionIDBytes || strings.IndexByte(id, 0) >= 0 {
		return ErrConnectorTransactionInvalid
	}
	return nil
}

func validateConnectorTransactionConnectorID(id string) error {
	if id == "" || strings.TrimSpace(id) != id || len(id) > MaxConnectorTransactionConnectorIDBytes || strings.IndexByte(id, 0) >= 0 {
		return ErrConnectorTransactionInvalid
	}
	return nil
}

func validateConnectorTransaction(transaction ConnectorTransaction) error {
	if validateConnectorTransactionID(transaction.ID) != nil || validateConnectorTransactionConnectorID(transaction.ConnectorID) != nil || transaction.Generation == 0 || transaction.Sequence == 0 || transaction.CreatedSequence == 0 || transaction.CreatedSequence > transaction.Sequence || transaction.Attempt == 0 || transaction.Attempt > MaxConnectorTransactionAttempts || len(transaction.Offset) > MaxConnectorTransactionOffsetBytes || len(transaction.Frontier) > MaxConnectorTransactionFrontierBytes || len(transaction.LastError) > MaxConnectorTransactionErrorBytes || strings.IndexByte(transaction.LastError, 0) >= 0 || transaction.State < ConnectorTransactionPending || transaction.State > ConnectorTransactionAborted {
		return ErrConnectorTransactionInvalid
	}
	if transaction.State != ConnectorTransactionFailed && transaction.LastError != "" {
		return ErrConnectorTransactionInvalid
	}
	return nil
}

func validateConnectorTransactionSnapshot(snapshot ConnectorTransactionSnapshot) error {
	if len(snapshot.Transactions) > MaxConnectorTransactionJournalCapacity {
		return ErrConnectorTransactionCorrupt
	}
	seen := make(map[string]struct{}, len(snapshot.Transactions))
	for _, transaction := range snapshot.Transactions {
		if transaction.Sequence > snapshot.Revision || transaction.CreatedSequence > snapshot.Revision {
			return ErrConnectorTransactionCorrupt
		}
		if err := validateConnectorTransaction(transaction); err != nil {
			return ErrConnectorTransactionCorrupt
		}
		if _, exists := seen[transaction.ID]; exists {
			return ErrConnectorTransactionCorrupt
		}
		seen[transaction.ID] = struct{}{}
	}
	return nil
}

func connectorTransactionMatchesIntent(transaction ConnectorTransaction, intent ConnectorTransactionIntent) bool {
	return transaction.ConnectorID == intent.ConnectorID && transaction.Generation == intent.Generation && bytes.Equal(transaction.Offset, intent.Offset) && bytes.Equal(transaction.Frontier, intent.Frontier)
}

func (journal *ConnectorTransactionJournal) findLocked(id string) int {
	for index := range journal.transactions {
		if journal.transactions[index].ID == id {
			return index
		}
	}
	return -1
}

func (journal *ConnectorTransactionJournal) lookupLocked(id string, generation uint64) (int, error) {
	index := journal.findLocked(id)
	if index < 0 {
		return -1, ErrConnectorTransactionNotFound
	}
	if journal.transactions[index].Generation != generation {
		return -1, ErrConnectorTransactionGenerationMismatch
	}
	return index, nil
}

func (journal *ConnectorTransactionJournal) firstTerminalLocked() int {
	for index, transaction := range journal.transactions {
		if transaction.State == ConnectorTransactionCommitted || transaction.State == ConnectorTransactionAborted {
			return index
		}
	}
	return -1
}

func (journal *ConnectorTransactionJournal) advanceLocked(transaction *ConnectorTransaction) error {
	if journal.revision == ^uint64(0) {
		return ErrConnectorTransactionJournalOptions
	}
	journal.revision++
	transaction.Sequence = journal.revision
	return nil
}

func (journal *ConnectorTransactionJournal) captureLocked() ConnectorTransactionSnapshot {
	transactions := make([]ConnectorTransaction, len(journal.transactions))
	for index, transaction := range journal.transactions {
		transactions[index] = cloneConnectorTransaction(transaction)
	}
	return ConnectorTransactionSnapshot{Revision: journal.revision, Dropped: journal.dropped, Transactions: transactions}
}

func (journal *ConnectorTransactionJournal) restoreLocked(snapshot ConnectorTransactionSnapshot) {
	journal.revision = snapshot.Revision
	journal.dropped = snapshot.Dropped
	journal.transactions = make([]ConnectorTransaction, len(snapshot.Transactions), journal.capacity)
	for index, transaction := range snapshot.Transactions {
		journal.transactions[index] = cloneConnectorTransaction(transaction)
	}
}

func (journal *ConnectorTransactionJournal) persistLocked(ctx context.Context) error {
	if journal.store == nil {
		return nil
	}
	payload, err := EncodeConnectorTransactionSnapshot(journal.captureLocked())
	if err != nil {
		return err
	}
	return journal.store.Save(ctx, payload)
}

func cloneConnectorTransaction(transaction ConnectorTransaction) ConnectorTransaction {
	transaction.Offset = append([]byte(nil), transaction.Offset...)
	transaction.Frontier = append([]byte(nil), transaction.Frontier...)
	return transaction
}
