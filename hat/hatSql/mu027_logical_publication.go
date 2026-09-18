package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultSQLPublicationMaxHistoryBatches bounds replayable publication
	// history when no explicit option is supplied.
	DefaultSQLPublicationMaxHistoryBatches = 256
	// DefaultSQLPublicationMaxBatchDeltas bounds one publication batch.
	DefaultSQLPublicationMaxBatchDeltas = 4096
	// DefaultSQLPublicationMaxPendingBatches bounds live subscriber buffering.
	DefaultSQLPublicationMaxPendingBatches = 64
	// DefaultSQLPublicationMaxSubscribers bounds active subscribers.
	DefaultSQLPublicationMaxSubscribers = 1024
	// DefaultSQLPublicationMaxColumns bounds the fixed publication schema.
	DefaultSQLPublicationMaxColumns = 256
	// DefaultSQLPublicationMaxRowColumns bounds one differential row.
	DefaultSQLPublicationMaxRowColumns = 256

	maxSQLPublicationHistoryBatches = 1 << 16
	maxSQLPublicationBatchDeltas    = 1 << 16
	maxSQLPublicationPendingBatches = 1 << 16
	maxSQLPublicationSubscribers    = 1 << 16
	maxSQLPublicationColumns        = 1 << 12
	maxSQLPublicationRowColumns     = 1 << 12
	maxSQLPublicationTextBytes      = 1 << 20

	sqlPublicationCheckpointSize = 24
)

var (
	// ErrSQLPublicationInvalid indicates malformed publication input or
	// unsupported options.
	ErrSQLPublicationInvalid = errors.New("hatSql: invalid SQL publication input")
	// ErrSQLPublicationLimit indicates that a configured publication bound was
	// exceeded.
	ErrSQLPublicationLimit = errors.New("hatSql: SQL publication limit exceeded")
	// ErrSQLPublicationSequence indicates a non-contiguous revision or a
	// checkpoint that moves in the wrong direction.
	ErrSQLPublicationSequence = errors.New("hatSql: invalid SQL publication sequence")
	// ErrSQLPublicationCheckpointExpired indicates that the requested revision
	// is older than retained history and needs a fresh snapshot.
	ErrSQLPublicationCheckpointExpired = errors.New("hatSql: SQL publication checkpoint expired")
	// ErrSQLPublicationBackpressure indicates that a subscriber could not keep
	// up and was closed without blocking the publisher.
	ErrSQLPublicationBackpressure = errors.New("hatSql: SQL publication subscriber backpressure")
	// ErrSQLPublicationClosed indicates that a publication no longer accepts
	// new batches.
	ErrSQLPublicationClosed = errors.New("hatSql: SQL publication closed")
)

// SQLPublicationOptions controls bounded history, batches, subscribers, and
// schema sizes. Zero values select the documented defaults; negative values
// and excessive configurations are rejected.
type SQLPublicationOptions struct {
	MaxHistoryBatches int
	MaxBatchDeltas    int
	MaxPendingBatches int
	MaxSubscribers    int
	MaxColumns        int
	MaxRowColumns     int
}

type normalizedSQLPublicationOptions struct {
	maxHistoryBatches int
	maxBatchDeltas    int
	maxPendingBatches int
	maxSubscribers    int
	maxColumns        int
	maxRowColumns     int
}

// SQLPublicationDelta is one signed multiplicity change for a published SQL
// row. A positive Diff inserts a row and a negative Diff retracts it.
type SQLPublicationDelta struct {
	Row  Row   `json:"row"`
	Diff int64 `json:"diff"`
}

// SQLPublicationBatch is one versioned logical publication update. Columns may
// be omitted by callers after construction; the publication fills the fixed
// schema into stored and delivered batches.
type SQLPublicationBatch struct {
	Revision uint64                `json:"revision"`
	Frontier uint64                `json:"frontier"`
	Columns  []string              `json:"columns,omitempty"`
	Deltas   []SQLPublicationDelta `json:"deltas,omitempty"`
	Progress bool                  `json:"progress,omitempty"`
	Complete bool                  `json:"complete,omitempty"`
	Reset    bool                  `json:"reset,omitempty"`
}

// SQLPublicationCheckpoint identifies the last publication version durably
// accepted by a consumer. Checkpoints are publication-specific and should be
// persisted only after the downstream side effect is durable.
type SQLPublicationCheckpoint struct {
	Revision uint64 `json:"revision"`
	Frontier uint64 `json:"frontier"`
}

// SQLPublicationSnapshot describes a publication without exposing mutable
// history or subscriber state.
type SQLPublicationSnapshot struct {
	Name             string   `json:"name"`
	Columns          []string `json:"columns"`
	EarliestRevision uint64   `json:"earliest_revision"`
	LatestRevision   uint64   `json:"latest_revision"`
	LatestFrontier   uint64   `json:"latest_frontier"`
	HistoryBatches   int      `json:"history_batches"`
	Subscribers      int      `json:"subscribers"`
	Closed           bool     `json:"closed"`
}

type sqlPublicationSubscriber struct {
	id                uint64
	updates           chan SQLPublicationBatch
	done              chan struct{}
	checkpoint        SQLPublicationCheckpoint
	deliveredRevision uint64
	deliveredFrontier uint64
	err               error
	closed            bool
}

// SQLPublication is a bounded in-memory logical SQL change publication. It
// provides contiguous revisions, retained replay, explicit consumer
// checkpoints, and bounded non-blocking subscriber delivery. It does not
// persist data or checkpoints by itself; callers own durable storage and
// connector exactly-once coordination.
type SQLPublication struct {
	mu sync.Mutex

	name        string
	columns     []string
	columnSet   map[string]struct{}
	options     normalizedSQLPublicationOptions
	history     []SQLPublicationBatch
	latest      SQLPublicationCheckpoint
	subscribers map[uint64]*sqlPublicationSubscriber
	nextID      uint64
	closed      bool
}

// SQLPublicationSubscription is a replay-plus-live consumer handle.
type SQLPublicationSubscription struct {
	publication *SQLPublication
	state       *sqlPublicationSubscriber
}

// NewSQLPublication creates a named publication with one fixed column schema.
func NewSQLPublication(name string, columns []string, options SQLPublicationOptions) (*SQLPublication, error) {
	normalized, err := normalizeSQLPublicationOptions(options)
	if err != nil {
		return nil, err
	}
	if err := validateSQLPublicationText(name, maxSQLPublicationTextBytes, "publication name"); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: publication requires at least one column", ErrSQLPublicationInvalid)
	}
	if len(columns) > normalized.maxColumns {
		return nil, fmt.Errorf("%w: columns", ErrSQLPublicationLimit)
	}
	copiedColumns := append([]string(nil), columns...)
	columnSet := make(map[string]struct{}, len(copiedColumns))
	for _, column := range copiedColumns {
		if err := validateSQLPublicationText(column, maxSQLPublicationTextBytes, "column name"); err != nil {
			return nil, err
		}
		if _, exists := columnSet[column]; exists {
			return nil, fmt.Errorf("%w: duplicate column %q", ErrSQLPublicationInvalid, column)
		}
		columnSet[column] = struct{}{}
	}
	return &SQLPublication{
		name:        name,
		columns:     copiedColumns,
		columnSet:   columnSet,
		options:     normalized,
		history:     make([]SQLPublicationBatch, 0, normalized.maxHistoryBatches),
		subscribers: make(map[uint64]*sqlPublicationSubscriber),
	}, nil
}

// Name returns the immutable publication name.
func (publication *SQLPublication) Name() string {
	if publication == nil {
		return ""
	}
	return publication.name
}

// Columns returns an independent copy of the immutable publication schema.
func (publication *SQLPublication) Columns() []string {
	if publication == nil {
		return nil
	}
	return append([]string(nil), publication.columns...)
}

// Append validates and publishes the next contiguous logical batch. A slow
// subscriber is closed and causes ErrSQLPublicationBackpressure after the
// batch has been retained for later replay or resynchronization.
func (publication *SQLPublication) Append(batch SQLPublicationBatch) error {
	if publication == nil {
		return fmt.Errorf("%w: nil publication", ErrSQLPublicationInvalid)
	}
	publication.mu.Lock()
	defer publication.mu.Unlock()
	if publication.closed {
		return ErrSQLPublicationClosed
	}
	normalized, err := publication.normalizeBatchLocked(batch)
	if err != nil {
		return err
	}
	publication.appendHistoryLocked(normalized)
	publication.latest = SQLPublicationCheckpoint{
		Revision: normalized.Revision,
		Frontier: normalized.Frontier,
	}

	backpressure := false
	for id, subscriber := range publication.subscribers {
		if subscriber.closed {
			delete(publication.subscribers, id)
			continue
		}
		select {
		case subscriber.updates <- cloneSQLPublicationBatch(normalized):
			subscriber.deliveredRevision = normalized.Revision
			subscriber.deliveredFrontier = normalized.Frontier
		default:
			publication.closeSubscriberLocked(subscriber, ErrSQLPublicationBackpressure)
			delete(publication.subscribers, id)
			backpressure = true
		}
	}
	if normalized.Complete {
		publication.closed = true
		for id, subscriber := range publication.subscribers {
			publication.closeSubscriberLocked(subscriber, nil)
			delete(publication.subscribers, id)
		}
	}
	if backpressure {
		return ErrSQLPublicationBackpressure
	}
	return nil
}

// Subscribe creates a replay-plus-live subscription beginning after the
// supplied checkpoint. A zero checkpoint starts at the oldest retained batch.
func (publication *SQLPublication) Subscribe(ctx context.Context, checkpoint SQLPublicationCheckpoint) (*SQLPublicationSubscription, error) {
	if publication == nil {
		return nil, fmt.Errorf("%w: nil publication", ErrSQLPublicationInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	publication.mu.Lock()
	defer publication.mu.Unlock()
	if err := publication.validateCheckpointLocked(checkpoint, false); err != nil {
		return nil, err
	}
	replay := publication.replayAfterLocked(checkpoint.Revision)
	capacity := len(replay) + publication.options.maxPendingBatches
	if capacity < len(replay) || capacity < publication.options.maxPendingBatches {
		return nil, fmt.Errorf("%w: subscriber buffer", ErrSQLPublicationLimit)
	}
	if !publication.closed && len(publication.subscribers) >= publication.options.maxSubscribers {
		return nil, fmt.Errorf("%w: subscribers", ErrSQLPublicationLimit)
	}
	subscriber := &sqlPublicationSubscriber{
		updates:           make(chan SQLPublicationBatch, capacity),
		done:              make(chan struct{}),
		checkpoint:        checkpoint,
		deliveredRevision: checkpoint.Revision,
		deliveredFrontier: checkpoint.Frontier,
	}
	for _, batch := range replay {
		subscriber.updates <- cloneSQLPublicationBatch(batch)
		subscriber.deliveredRevision = batch.Revision
		subscriber.deliveredFrontier = batch.Frontier
		if batch.Complete {
			publication.closeSubscriberLocked(subscriber, nil)
			return &SQLPublicationSubscription{publication: publication, state: subscriber}, nil
		}
	}
	if publication.closed {
		publication.closeSubscriberLocked(subscriber, nil)
		return &SQLPublicationSubscription{publication: publication, state: subscriber}, nil
	}
	publication.nextID++
	subscriber.id = publication.nextID
	publication.subscribers[subscriber.id] = subscriber
	return &SQLPublicationSubscription{publication: publication, state: subscriber}, nil
}

// Snapshot returns bounded publication metadata and a copy of its schema.
func (publication *SQLPublication) Snapshot() SQLPublicationSnapshot {
	if publication == nil {
		return SQLPublicationSnapshot{}
	}
	publication.mu.Lock()
	defer publication.mu.Unlock()
	snapshot := SQLPublicationSnapshot{
		Name:           publication.name,
		Columns:        append([]string(nil), publication.columns...),
		LatestRevision: publication.latest.Revision,
		LatestFrontier: publication.latest.Frontier,
		HistoryBatches: len(publication.history),
		Subscribers:    len(publication.subscribers),
		Closed:         publication.closed,
	}
	if len(publication.history) > 0 {
		snapshot.EarliestRevision = publication.history[0].Revision
	}
	return snapshot
}

// Close stops accepting batches and closes active subscriptions with
// ErrSQLPublicationClosed. A completed publication instead closes normally.
func (publication *SQLPublication) Close() {
	if publication == nil {
		return
	}
	publication.mu.Lock()
	defer publication.mu.Unlock()
	if publication.closed {
		return
	}
	publication.closed = true
	for id, subscriber := range publication.subscribers {
		publication.closeSubscriberLocked(subscriber, ErrSQLPublicationClosed)
		delete(publication.subscribers, id)
	}
}

// Updates returns the bounded replay-plus-live batch channel.
func (subscription *SQLPublicationSubscription) Updates() <-chan SQLPublicationBatch {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.updates
}

// Done returns a channel closed when the subscription ends.
func (subscription *SQLPublicationSubscription) Done() <-chan struct{} {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.done
}

// Ack advances the consumer checkpoint monotonically. Call it only after the
// downstream side effect represented by the checkpoint is durable.
func (subscription *SQLPublicationSubscription) Ack(checkpoint SQLPublicationCheckpoint) error {
	if subscription == nil || subscription.publication == nil || subscription.state == nil {
		return fmt.Errorf("%w: nil subscription", ErrSQLPublicationInvalid)
	}
	publication := subscription.publication
	publication.mu.Lock()
	defer publication.mu.Unlock()
	if subscription.state.closed && subscription.state.err != nil {
		return subscription.state.err
	}
	if err := publication.validateCheckpointLocked(checkpoint, true); err != nil {
		return err
	}
	if checkpoint.Revision > subscription.state.deliveredRevision {
		return fmt.Errorf("%w: checkpoint was not delivered", ErrSQLPublicationSequence)
	}
	if checkpoint.Revision < subscription.state.checkpoint.Revision ||
		(checkpoint.Revision == subscription.state.checkpoint.Revision && checkpoint.Frontier < subscription.state.checkpoint.Frontier) {
		return fmt.Errorf("%w: checkpoint moved backwards", ErrSQLPublicationSequence)
	}
	subscription.state.checkpoint = checkpoint
	return nil
}

// Checkpoint returns the last acknowledged consumer checkpoint.
func (subscription *SQLPublicationSubscription) Checkpoint() SQLPublicationCheckpoint {
	if subscription == nil || subscription.publication == nil || subscription.state == nil {
		return SQLPublicationCheckpoint{}
	}
	subscription.publication.mu.Lock()
	defer subscription.publication.mu.Unlock()
	return subscription.state.checkpoint
}

// Err returns the terminal subscription error. Normal Close and Complete
// termination return nil.
func (subscription *SQLPublicationSubscription) Err() error {
	if subscription == nil || subscription.publication == nil || subscription.state == nil {
		return nil
	}
	subscription.publication.mu.Lock()
	defer subscription.publication.mu.Unlock()
	return subscription.state.err
}

// Close removes the subscription. It is idempotent and does not report an
// error to the consumer.
func (subscription *SQLPublicationSubscription) Close() {
	if subscription == nil || subscription.publication == nil || subscription.state == nil {
		return
	}
	publication := subscription.publication
	publication.mu.Lock()
	defer publication.mu.Unlock()
	if subscription.state.closed {
		return
	}
	publication.closeSubscriberLocked(subscription.state, nil)
	delete(publication.subscribers, subscription.state.id)
}

// MarshalBinary encodes a checkpoint as a fixed 24-byte versioned CRC32
// record suitable for a small durable consumer checkpoint file.
func (checkpoint SQLPublicationCheckpoint) MarshalBinary() ([]byte, error) {
	encoded := make([]byte, sqlPublicationCheckpointSize)
	copy(encoded[:4], []byte("HPC1"))
	binary.BigEndian.PutUint64(encoded[4:12], checkpoint.Revision)
	binary.BigEndian.PutUint64(encoded[12:20], checkpoint.Frontier)
	binary.BigEndian.PutUint32(encoded[20:24], crc32.ChecksumIEEE(encoded[:20]))
	return encoded, nil
}

// UnmarshalBinary validates and decodes a fixed-format checkpoint.
func (checkpoint *SQLPublicationCheckpoint) UnmarshalBinary(encoded []byte) error {
	if checkpoint == nil {
		return fmt.Errorf("%w: nil checkpoint", ErrSQLPublicationInvalid)
	}
	if len(encoded) != sqlPublicationCheckpointSize || string(encoded[:4]) != "HPC1" {
		return fmt.Errorf("%w: checkpoint header", ErrSQLPublicationInvalid)
	}
	if want, got := binary.BigEndian.Uint32(encoded[20:24]), crc32.ChecksumIEEE(encoded[:20]); want != got {
		return fmt.Errorf("%w: checkpoint checksum", ErrSQLPublicationInvalid)
	}
	checkpoint.Revision = binary.BigEndian.Uint64(encoded[4:12])
	checkpoint.Frontier = binary.BigEndian.Uint64(encoded[12:20])
	return nil
}

func normalizeSQLPublicationOptions(options SQLPublicationOptions) (normalizedSQLPublicationOptions, error) {
	values := [...]int{
		options.MaxHistoryBatches,
		options.MaxBatchDeltas,
		options.MaxPendingBatches,
		options.MaxSubscribers,
		options.MaxColumns,
		options.MaxRowColumns,
	}
	for _, value := range values {
		if value < 0 {
			return normalizedSQLPublicationOptions{}, fmt.Errorf("%w: negative option", ErrSQLPublicationInvalid)
		}
	}
	normalized := normalizedSQLPublicationOptions{
		maxHistoryBatches: defaultSQLPublicationOption(options.MaxHistoryBatches, DefaultSQLPublicationMaxHistoryBatches),
		maxBatchDeltas:    defaultSQLPublicationOption(options.MaxBatchDeltas, DefaultSQLPublicationMaxBatchDeltas),
		maxPendingBatches: defaultSQLPublicationOption(options.MaxPendingBatches, DefaultSQLPublicationMaxPendingBatches),
		maxSubscribers:    defaultSQLPublicationOption(options.MaxSubscribers, DefaultSQLPublicationMaxSubscribers),
		maxColumns:        defaultSQLPublicationOption(options.MaxColumns, DefaultSQLPublicationMaxColumns),
		maxRowColumns:     defaultSQLPublicationOption(options.MaxRowColumns, DefaultSQLPublicationMaxRowColumns),
	}
	maximums := [...]int{
		maxSQLPublicationHistoryBatches,
		maxSQLPublicationBatchDeltas,
		maxSQLPublicationPendingBatches,
		maxSQLPublicationSubscribers,
		maxSQLPublicationColumns,
		maxSQLPublicationRowColumns,
	}
	values = [...]int{
		normalized.maxHistoryBatches,
		normalized.maxBatchDeltas,
		normalized.maxPendingBatches,
		normalized.maxSubscribers,
		normalized.maxColumns,
		normalized.maxRowColumns,
	}
	for index, value := range values {
		if value > maximums[index] {
			return normalizedSQLPublicationOptions{}, fmt.Errorf("%w: option %d is too large", ErrSQLPublicationInvalid, index)
		}
	}
	return normalized, nil
}

func defaultSQLPublicationOption(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func (publication *SQLPublication) normalizeBatchLocked(batch SQLPublicationBatch) (SQLPublicationBatch, error) {
	wantRevision := publication.latest.Revision + 1
	if publication.latest.Revision == ^uint64(0) || batch.Revision != wantRevision {
		return SQLPublicationBatch{}, fmt.Errorf("%w: got revision %d, want %d", ErrSQLPublicationSequence, batch.Revision, wantRevision)
	}
	if batch.Frontier < publication.latest.Frontier {
		return SQLPublicationBatch{}, fmt.Errorf("%w: frontier moved backwards", ErrSQLPublicationSequence)
	}
	if len(batch.Deltas) > publication.options.maxBatchDeltas {
		return SQLPublicationBatch{}, fmt.Errorf("%w: batch deltas", ErrSQLPublicationLimit)
	}
	if len(batch.Columns) > 0 && !sameSQLPublicationColumns(batch.Columns, publication.columns) {
		return SQLPublicationBatch{}, fmt.Errorf("%w: publication schema changed", ErrSQLPublicationInvalid)
	}
	normalized := SQLPublicationBatch{
		Revision: batch.Revision,
		Frontier: batch.Frontier,
		Columns:  append([]string(nil), publication.columns...),
		Progress: batch.Progress,
		Complete: batch.Complete,
		Reset:    batch.Reset,
		Deltas:   make([]SQLPublicationDelta, len(batch.Deltas)),
	}
	for index, delta := range batch.Deltas {
		if delta.Diff == 0 {
			return SQLPublicationBatch{}, fmt.Errorf("%w: zero delta", ErrSQLPublicationInvalid)
		}
		row, err := publication.copyAndValidateRow(delta.Row)
		if err != nil {
			return SQLPublicationBatch{}, err
		}
		normalized.Deltas[index] = SQLPublicationDelta{Row: row, Diff: delta.Diff}
	}
	return normalized, nil
}

func (publication *SQLPublication) copyAndValidateRow(row Row) (Row, error) {
	if len(row) > publication.options.maxRowColumns {
		return nil, fmt.Errorf("%w: row columns", ErrSQLPublicationLimit)
	}
	if row == nil {
		return nil, nil
	}
	copied := make(Row, len(row))
	for column, value := range row {
		if _, exists := publication.columnSet[column]; !exists {
			return nil, fmt.Errorf("%w: row column %q is not in publication schema", ErrSQLPublicationInvalid, column)
		}
		copied[column] = value
	}
	return copied, nil
}

func (publication *SQLPublication) appendHistoryLocked(batch SQLPublicationBatch) {
	if len(publication.history) < publication.options.maxHistoryBatches {
		publication.history = append(publication.history, batch)
		return
	}
	copy(publication.history, publication.history[1:])
	publication.history[len(publication.history)-1] = batch
}

func (publication *SQLPublication) replayAfterLocked(revision uint64) []SQLPublicationBatch {
	if len(publication.history) == 0 {
		return nil
	}
	start := len(publication.history)
	for index, batch := range publication.history {
		if batch.Revision > revision {
			start = index
			break
		}
	}
	return publication.history[start:]
}

func (publication *SQLPublication) validateCheckpointLocked(checkpoint SQLPublicationCheckpoint, forAck bool) error {
	if checkpoint.Revision == 0 {
		if checkpoint.Frontier != 0 {
			return fmt.Errorf("%w: non-zero frontier without revision", ErrSQLPublicationInvalid)
		}
		return nil
	}
	if checkpoint.Revision > publication.latest.Revision || checkpoint.Frontier > publication.latest.Frontier {
		return fmt.Errorf("%w: checkpoint is ahead of publication", ErrSQLPublicationSequence)
	}
	if len(publication.history) > 0 {
		earliest := publication.history[0].Revision
		if checkpoint.Revision+1 < earliest {
			return ErrSQLPublicationCheckpointExpired
		}
		for _, batch := range publication.history {
			if batch.Revision == checkpoint.Revision && batch.Frontier != checkpoint.Frontier {
				return fmt.Errorf("%w: checkpoint frontier mismatch", ErrSQLPublicationSequence)
			}
		}
	} else if checkpoint.Revision < publication.latest.Revision {
		return ErrSQLPublicationCheckpointExpired
	}
	if forAck && checkpoint.Revision == publication.latest.Revision && checkpoint.Frontier != publication.latest.Frontier {
		return fmt.Errorf("%w: checkpoint frontier mismatch", ErrSQLPublicationSequence)
	}
	return nil
}

func (publication *SQLPublication) closeSubscriberLocked(subscriber *sqlPublicationSubscriber, err error) {
	if subscriber == nil || subscriber.closed {
		return
	}
	subscriber.closed = true
	subscriber.err = err
	close(subscriber.updates)
	close(subscriber.done)
}

func cloneSQLPublicationBatch(batch SQLPublicationBatch) SQLPublicationBatch {
	cloned := batch
	cloned.Columns = append([]string(nil), batch.Columns...)
	cloned.Deltas = make([]SQLPublicationDelta, len(batch.Deltas))
	for index, delta := range batch.Deltas {
		cloned.Deltas[index] = SQLPublicationDelta{
			Row:  cloneSQLPublicationRow(delta.Row),
			Diff: delta.Diff,
		}
	}
	return cloned
}

func cloneSQLPublicationRow(row Row) Row {
	if row == nil {
		return nil
	}
	cloned := make(Row, len(row))
	for key, value := range row {
		cloned[key] = value
	}
	return cloned
}

func sameSQLPublicationColumns(left, right []string) bool {
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

func validateSQLPublicationText(value string, maxBytes int, field string) error {
	if value == "" {
		return fmt.Errorf("%w: empty %s", ErrSQLPublicationInvalid, field)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%w: %s is too long", ErrSQLPublicationLimit, field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrSQLPublicationInvalid, field)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: %s contains a control character", ErrSQLPublicationInvalid, field)
		}
	}
	return nil
}
