package hatDataStructure

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

const (
	// DefaultSpaceChangefeedMaxHistoryBatches bounds replayable history when
	// no explicit option is supplied.
	DefaultSpaceChangefeedMaxHistoryBatches = 256
	// DefaultSpaceChangefeedMaxBatchChanges bounds one published batch.
	DefaultSpaceChangefeedMaxBatchChanges = 4096
	// DefaultSpaceChangefeedMaxPendingBatches bounds live subscriber buffering.
	DefaultSpaceChangefeedMaxPendingBatches = 64
	// DefaultSpaceChangefeedMaxSubscribers bounds active subscribers.
	DefaultSpaceChangefeedMaxSubscribers = 1024
	// DefaultSpaceChangefeedMaxChangeBytes bounds the total payload in one
	// published batch.
	DefaultSpaceChangefeedMaxChangeBytes = 4 << 20

	maxSpaceChangefeedHistoryBatches = 1 << 16
	maxSpaceChangefeedBatchChanges   = 1 << 16
	maxSpaceChangefeedPendingBatches = 1 << 16
	maxSpaceChangefeedSubscribers    = 1 << 16
	maxSpaceChangefeedChangeBytes    = 1 << 30
	maxSpaceChangefeedTextBytes      = 1 << 20

	spaceChangefeedCheckpointSize = 32
)

var (
	// ErrSpaceChangefeedInvalid indicates malformed feed input or unsupported
	// options.
	ErrSpaceChangefeedInvalid = errors.New("hatDataStructure: invalid space changefeed input")
	// ErrSpaceChangefeedLimit indicates that a configured feed bound was
	// exceeded.
	ErrSpaceChangefeedLimit = errors.New("hatDataStructure: space changefeed limit exceeded")
	// ErrSpaceChangefeedSequence indicates a non-contiguous sequence or a
	// checkpoint that moves in the wrong direction.
	ErrSpaceChangefeedSequence = errors.New("hatDataStructure: invalid space changefeed sequence")
	// ErrSpaceChangefeedCheckpointExpired indicates that retained history is
	// newer than the requested checkpoint.
	ErrSpaceChangefeedCheckpointExpired = errors.New("hatDataStructure: space changefeed checkpoint expired")
	// ErrSpaceChangefeedBackpressure indicates that a subscriber was closed
	// because it did not accept a published batch.
	ErrSpaceChangefeedBackpressure = errors.New("hatDataStructure: space changefeed subscriber backpressure")
	// ErrSpaceChangefeedClosed indicates that the feed no longer accepts
	// batches.
	ErrSpaceChangefeedClosed = errors.New("hatDataStructure: space changefeed closed")

	spaceChangefeedCRCTable = crc32.MakeTable(crc32.Castagnoli)
)

// SpaceChangefeedOperation identifies one row transition in a space feed.
type SpaceChangefeedOperation uint8

const (
	SpaceChangeInsert SpaceChangefeedOperation = iota + 1
	SpaceChangeUpdate
	SpaceChangeDelete
)

// SpaceChange is an immutable-on-publication key and optional before/after
// image. The feed copies all byte slices when a batch is accepted and when it
// is delivered to a subscriber.
type SpaceChange struct {
	Operation SpaceChangefeedOperation
	Key       []byte
	Before    []byte
	After     []byte
}

// SpaceChangefeedBatch is one schema-bound, contiguous feed update.
type SpaceChangefeedBatch struct {
	Sequence      uint64
	Frontier      uint64
	SchemaVersion uint64
	Changes       []SpaceChange
	Snapshot      bool
	Progress      bool
	Complete      bool
}

// SpaceChangefeedCheckpoint identifies the last batch durably accepted by a
// consumer. Persist it only after the downstream side effect is durable.
type SpaceChangefeedCheckpoint struct {
	Sequence      uint64
	Frontier      uint64
	SchemaVersion uint64
}

// MarshalBinary encodes a compact SCF1 checkpoint with CRC32C protection.
func (checkpoint SpaceChangefeedCheckpoint) MarshalBinary() ([]byte, error) {
	if checkpoint.Sequence == 0 && (checkpoint.Frontier != 0 || checkpoint.SchemaVersion != 0) {
		return nil, fmt.Errorf("%w: zero checkpoint", ErrSpaceChangefeedInvalid)
	}
	if checkpoint.Sequence != 0 && checkpoint.SchemaVersion == 0 {
		return nil, fmt.Errorf("%w: checkpoint schema version", ErrSpaceChangefeedInvalid)
	}
	encoded := make([]byte, spaceChangefeedCheckpointSize)
	copy(encoded[:4], []byte("SCF1"))
	binary.LittleEndian.PutUint64(encoded[4:12], checkpoint.Sequence)
	binary.LittleEndian.PutUint64(encoded[12:20], checkpoint.Frontier)
	binary.LittleEndian.PutUint64(encoded[20:28], checkpoint.SchemaVersion)
	binary.LittleEndian.PutUint32(encoded[28:], crc32.Checksum(encoded[:28], spaceChangefeedCRCTable))
	return encoded, nil
}

// UnmarshalBinary decodes and validates an SCF1 checkpoint.
func (checkpoint *SpaceChangefeedCheckpoint) UnmarshalBinary(encoded []byte) error {
	if checkpoint == nil {
		return fmt.Errorf("%w: nil checkpoint", ErrSpaceChangefeedInvalid)
	}
	if len(encoded) != spaceChangefeedCheckpointSize || string(encoded[:4]) != "SCF1" {
		return fmt.Errorf("%w: checkpoint frame", ErrSpaceChangefeedInvalid)
	}
	want := binary.LittleEndian.Uint32(encoded[28:])
	got := crc32.Checksum(encoded[:28], spaceChangefeedCRCTable)
	if want != got {
		return fmt.Errorf("%w: checkpoint checksum", ErrSpaceChangefeedInvalid)
	}
	decoded := SpaceChangefeedCheckpoint{
		Sequence:      binary.LittleEndian.Uint64(encoded[4:12]),
		Frontier:      binary.LittleEndian.Uint64(encoded[12:20]),
		SchemaVersion: binary.LittleEndian.Uint64(encoded[20:28]),
	}
	if decoded.Sequence == 0 && (decoded.Frontier != 0 || decoded.SchemaVersion != 0) {
		return fmt.Errorf("%w: zero checkpoint", ErrSpaceChangefeedInvalid)
	}
	if decoded.Sequence != 0 && decoded.SchemaVersion == 0 {
		return fmt.Errorf("%w: checkpoint schema version", ErrSpaceChangefeedInvalid)
	}
	*checkpoint = decoded
	return nil
}

// SpaceChangefeedOptions controls retained history, batch size, subscriber
// buffering, subscriber count, and byte bounds. Zero values select defaults;
// negative values and excessive configurations are rejected.
type SpaceChangefeedOptions struct {
	MaxHistoryBatches int
	MaxBatchChanges   int
	MaxPendingBatches int
	MaxSubscribers    int
	MaxChangeBytes    int
}

type normalizedSpaceChangefeedOptions struct {
	maxHistoryBatches int
	maxBatchChanges   int
	maxPendingBatches int
	maxSubscribers    int
	maxChangeBytes    int
}

// SpaceChangefeedSnapshot describes bounded feed metadata without exposing
// mutable history or subscriber state.
type SpaceChangefeedSnapshot struct {
	Name             string
	SchemaVersion    uint64
	EarliestSequence uint64
	LatestSequence   uint64
	LatestFrontier   uint64
	HistoryBatches   int
	Subscribers      int
	Closed           bool
}

type spaceChangefeedSubscriber struct {
	id                uint64
	updates           chan SpaceChangefeedBatch
	done              chan struct{}
	checkpoint        SpaceChangefeedCheckpoint
	deliveredSequence uint64
	deliveredFrontier uint64
	err               error
	closed            bool
}

type spaceChangefeedStoredChange struct {
	operation              SpaceChangefeedOperation
	keyStart, keyLength    int
	beforeStart, beforeLen int
	afterStart, afterLen   int
}

type spaceChangefeedStoredBatch struct {
	sequence      uint64
	frontier      uint64
	schemaVersion uint64
	changes       []spaceChangefeedStoredChange
	payload       []byte
	snapshot      bool
	progress      bool
	complete      bool
}

// SpaceChangefeed is a bounded named-space change publication. It provides
// contiguous schema-bound sequences, retained replay, explicit consumer
// checkpoints, and nonblocking slow-consumer eviction. It does not persist
// rows or checkpoints; callers own durable storage and application ordering.
type SpaceChangefeed struct {
	mu sync.Mutex

	name          string
	schemaVersion uint64
	options       normalizedSpaceChangefeedOptions
	history       []spaceChangefeedStoredBatch
	latest        SpaceChangefeedCheckpoint
	subscribers   map[uint64]*spaceChangefeedSubscriber
	nextID        uint64
	closed        bool
}

// SpaceChangefeedSubscription is a replay-plus-live consumer handle.
type SpaceChangefeedSubscription struct {
	feed  *SpaceChangefeed
	state *spaceChangefeedSubscriber
}

// NewSpaceChangefeed creates a schema-bound named-space feed.
func NewSpaceChangefeed(name string, schemaVersion uint64, options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	normalized, err := normalizeSpaceChangefeedOptions(options)
	if err != nil {
		return nil, err
	}
	if name == "" || len(name) > maxSpaceChangefeedTextBytes {
		return nil, fmt.Errorf("%w: feed name", ErrSpaceChangefeedInvalid)
	}
	if schemaVersion == 0 {
		return nil, fmt.Errorf("%w: schema version", ErrSpaceChangefeedInvalid)
	}
	return &SpaceChangefeed{
		name:          name,
		schemaVersion: schemaVersion,
		options:       normalized,
		history:       make([]spaceChangefeedStoredBatch, 0, normalized.maxHistoryBatches),
		subscribers:   make(map[uint64]*spaceChangefeedSubscriber),
	}, nil
}

// Name returns the immutable named-space identifier.
func (feed *SpaceChangefeed) Name() string {
	if feed == nil {
		return ""
	}
	return feed.name
}

// SchemaVersion returns the immutable schema version bound to this feed.
func (feed *SpaceChangefeed) SchemaVersion() uint64 {
	if feed == nil {
		return 0
	}
	return feed.schemaVersion
}

// Append validates and publishes the next contiguous batch. A slow
// subscriber is closed after the batch is retained, and backpressure is
// returned without blocking the publisher.
func (feed *SpaceChangefeed) Append(batch SpaceChangefeedBatch) error {
	if feed == nil {
		return fmt.Errorf("%w: nil feed", ErrSpaceChangefeedInvalid)
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return ErrSpaceChangefeedClosed
	}
	normalized, err := feed.normalizeBatchLocked(batch)
	if err != nil {
		return err
	}
	if len(feed.history) == feed.options.maxHistoryBatches {
		copy(feed.history, feed.history[1:])
		feed.history[len(feed.history)-1] = normalized
	} else {
		feed.history = append(feed.history, normalized)
	}
	feed.latest = SpaceChangefeedCheckpoint{
		Sequence:      normalized.sequence,
		Frontier:      normalized.frontier,
		SchemaVersion: normalized.schemaVersion,
	}

	backpressure := false
	for id, subscriber := range feed.subscribers {
		if subscriber.closed {
			delete(feed.subscribers, id)
			continue
		}
		select {
		case subscriber.updates <- cloneStoredSpaceChangefeedBatch(normalized):
			subscriber.deliveredSequence = normalized.sequence
			subscriber.deliveredFrontier = normalized.frontier
		default:
			feed.closeSubscriberLocked(subscriber, ErrSpaceChangefeedBackpressure)
			delete(feed.subscribers, id)
			backpressure = true
		}
	}
	if normalized.complete {
		feed.closed = true
		for id, subscriber := range feed.subscribers {
			feed.closeSubscriberLocked(subscriber, nil)
			delete(feed.subscribers, id)
		}
	}
	if backpressure {
		return ErrSpaceChangefeedBackpressure
	}
	return nil
}

// Subscribe creates a replay-plus-live subscription beginning after the
// supplied checkpoint. A zero checkpoint starts at the oldest retained batch.
func (feed *SpaceChangefeed) Subscribe(ctx context.Context, checkpoint SpaceChangefeedCheckpoint) (*SpaceChangefeedSubscription, error) {
	if feed == nil {
		return nil, fmt.Errorf("%w: nil feed", ErrSpaceChangefeedInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if err := feed.validateCheckpointLocked(checkpoint, false); err != nil {
		return nil, err
	}
	replay := feed.replayAfterLocked(checkpoint.Sequence)
	capacity := len(replay) + feed.options.maxPendingBatches
	if capacity < len(replay) || capacity < feed.options.maxPendingBatches {
		return nil, fmt.Errorf("%w: subscriber buffer", ErrSpaceChangefeedLimit)
	}
	if !feed.closed && len(feed.subscribers) >= feed.options.maxSubscribers {
		return nil, fmt.Errorf("%w: subscribers", ErrSpaceChangefeedLimit)
	}
	subscriber := &spaceChangefeedSubscriber{
		updates:           make(chan SpaceChangefeedBatch, capacity),
		done:              make(chan struct{}),
		checkpoint:        checkpoint,
		deliveredSequence: checkpoint.Sequence,
		deliveredFrontier: checkpoint.Frontier,
	}
	for _, batch := range replay {
		subscriber.updates <- cloneStoredSpaceChangefeedBatch(batch)
		subscriber.deliveredSequence = batch.sequence
		subscriber.deliveredFrontier = batch.frontier
		if batch.complete {
			feed.closeSubscriberLocked(subscriber, nil)
			return &SpaceChangefeedSubscription{feed: feed, state: subscriber}, nil
		}
	}
	if feed.closed {
		feed.closeSubscriberLocked(subscriber, nil)
		return &SpaceChangefeedSubscription{feed: feed, state: subscriber}, nil
	}
	feed.nextID++
	subscriber.id = feed.nextID
	feed.subscribers[subscriber.id] = subscriber
	return &SpaceChangefeedSubscription{feed: feed, state: subscriber}, nil
}

// Snapshot returns bounded feed metadata and immutable identity fields.
func (feed *SpaceChangefeed) Snapshot() SpaceChangefeedSnapshot {
	if feed == nil {
		return SpaceChangefeedSnapshot{}
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	snapshot := SpaceChangefeedSnapshot{
		Name:           feed.name,
		SchemaVersion:  feed.schemaVersion,
		LatestSequence: feed.latest.Sequence,
		LatestFrontier: feed.latest.Frontier,
		HistoryBatches: len(feed.history),
		Subscribers:    len(feed.subscribers),
		Closed:         feed.closed,
	}
	if len(feed.history) > 0 {
		snapshot.EarliestSequence = feed.history[0].sequence
	}
	return snapshot
}

// Close stops accepting batches and closes active subscriptions with a
// terminal error. A completed feed closes subscriptions normally.
func (feed *SpaceChangefeed) Close() {
	if feed == nil {
		return
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return
	}
	feed.closed = true
	for id, subscriber := range feed.subscribers {
		feed.closeSubscriberLocked(subscriber, ErrSpaceChangefeedClosed)
		delete(feed.subscribers, id)
	}
}

// Updates returns the bounded replay-plus-live batch channel.
func (subscription *SpaceChangefeedSubscription) Updates() <-chan SpaceChangefeedBatch {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.updates
}

// Done returns a channel closed when the subscription ends.
func (subscription *SpaceChangefeedSubscription) Done() <-chan struct{} {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.done
}

// Ack advances the consumer checkpoint monotonically after downstream work is
// durable. The checkpoint must refer to a delivered sequence.
func (subscription *SpaceChangefeedSubscription) Ack(checkpoint SpaceChangefeedCheckpoint) error {
	if subscription == nil || subscription.feed == nil || subscription.state == nil {
		return fmt.Errorf("%w: nil subscription", ErrSpaceChangefeedInvalid)
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.state.closed && subscription.state.err != nil {
		return subscription.state.err
	}
	if checkpoint.SchemaVersion != feed.schemaVersion || checkpoint.Sequence == 0 {
		return fmt.Errorf("%w: checkpoint schema or sequence", ErrSpaceChangefeedSequence)
	}
	if checkpoint.Sequence > subscription.state.deliveredSequence {
		return fmt.Errorf("%w: checkpoint was not delivered", ErrSpaceChangefeedSequence)
	}
	if checkpoint.Frontier > subscription.state.deliveredFrontier {
		return fmt.Errorf("%w: checkpoint frontier was not delivered", ErrSpaceChangefeedSequence)
	}
	if checkpoint.Sequence < subscription.state.checkpoint.Sequence ||
		(checkpoint.Sequence == subscription.state.checkpoint.Sequence && checkpoint.Frontier < subscription.state.checkpoint.Frontier) {
		return fmt.Errorf("%w: checkpoint moved backwards", ErrSpaceChangefeedSequence)
	}
	subscription.state.checkpoint = checkpoint
	return nil
}

// Checkpoint returns the last acknowledged consumer checkpoint.
func (subscription *SpaceChangefeedSubscription) Checkpoint() SpaceChangefeedCheckpoint {
	if subscription == nil || subscription.feed == nil || subscription.state == nil {
		return SpaceChangefeedCheckpoint{}
	}
	subscription.feed.mu.Lock()
	defer subscription.feed.mu.Unlock()
	return subscription.state.checkpoint
}

// Err returns the terminal subscription error. Normal Close and Complete
// termination return nil.
func (subscription *SpaceChangefeedSubscription) Err() error {
	if subscription == nil || subscription.feed == nil || subscription.state == nil {
		return nil
	}
	subscription.feed.mu.Lock()
	defer subscription.feed.mu.Unlock()
	return subscription.state.err
}

// Close removes the subscription. It is idempotent and does not report an
// error to the consumer.
func (subscription *SpaceChangefeedSubscription) Close() {
	if subscription == nil || subscription.feed == nil || subscription.state == nil {
		return
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.state.closed {
		return
	}
	delete(feed.subscribers, subscription.state.id)
	feed.closeSubscriberLocked(subscription.state, nil)
}

func normalizeSpaceChangefeedOptions(options SpaceChangefeedOptions) (normalizedSpaceChangefeedOptions, error) {
	values := []struct {
		value    int
		fallback int
		maximum  int
		name     string
	}{
		{options.MaxHistoryBatches, DefaultSpaceChangefeedMaxHistoryBatches, maxSpaceChangefeedHistoryBatches, "history batches"},
		{options.MaxBatchChanges, DefaultSpaceChangefeedMaxBatchChanges, maxSpaceChangefeedBatchChanges, "batch changes"},
		{options.MaxPendingBatches, DefaultSpaceChangefeedMaxPendingBatches, maxSpaceChangefeedPendingBatches, "pending batches"},
		{options.MaxSubscribers, DefaultSpaceChangefeedMaxSubscribers, maxSpaceChangefeedSubscribers, "subscribers"},
		{options.MaxChangeBytes, DefaultSpaceChangefeedMaxChangeBytes, maxSpaceChangefeedChangeBytes, "change bytes"},
	}
	for _, item := range values {
		if item.value < 0 || item.value > item.maximum {
			return normalizedSpaceChangefeedOptions{}, fmt.Errorf("%w: %s", ErrSpaceChangefeedInvalid, item.name)
		}
	}
	return normalizedSpaceChangefeedOptions{
		maxHistoryBatches: chooseSpaceChangefeedOption(options.MaxHistoryBatches, DefaultSpaceChangefeedMaxHistoryBatches),
		maxBatchChanges:   chooseSpaceChangefeedOption(options.MaxBatchChanges, DefaultSpaceChangefeedMaxBatchChanges),
		maxPendingBatches: chooseSpaceChangefeedOption(options.MaxPendingBatches, DefaultSpaceChangefeedMaxPendingBatches),
		maxSubscribers:    chooseSpaceChangefeedOption(options.MaxSubscribers, DefaultSpaceChangefeedMaxSubscribers),
		maxChangeBytes:    chooseSpaceChangefeedOption(options.MaxChangeBytes, DefaultSpaceChangefeedMaxChangeBytes),
	}, nil
}

func chooseSpaceChangefeedOption(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func (feed *SpaceChangefeed) normalizeBatchLocked(batch SpaceChangefeedBatch) (spaceChangefeedStoredBatch, error) {
	wantSequence := feed.latest.Sequence + 1
	if feed.latest.Sequence == ^uint64(0) || batch.Sequence != wantSequence {
		return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: want sequence %d", ErrSpaceChangefeedSequence, wantSequence)
	}
	if batch.SchemaVersion != feed.schemaVersion {
		return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: schema version", ErrSpaceChangefeedInvalid)
	}
	if batch.Frontier < feed.latest.Frontier {
		return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: frontier moved backwards", ErrSpaceChangefeedSequence)
	}
	if len(batch.Changes) > feed.options.maxBatchChanges {
		return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: batch changes", ErrSpaceChangefeedLimit)
	}
	bytes := 0
	for _, change := range batch.Changes {
		if change.Operation < SpaceChangeInsert || change.Operation > SpaceChangeDelete || len(change.Key) == 0 {
			return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: change", ErrSpaceChangefeedInvalid)
		}
		for _, payload := range [][]byte{change.Key, change.Before, change.After} {
			if len(payload) > feed.options.maxChangeBytes || bytes > feed.options.maxChangeBytes-len(payload) {
				return spaceChangefeedStoredBatch{}, fmt.Errorf("%w: change bytes", ErrSpaceChangefeedLimit)
			}
			bytes += len(payload)
		}
	}
	return storeSpaceChangefeedBatch(batch, bytes), nil
}

func (feed *SpaceChangefeed) validateCheckpointLocked(checkpoint SpaceChangefeedCheckpoint, allowExpired bool) error {
	if checkpoint.Sequence == 0 {
		if checkpoint.Frontier != 0 || checkpoint.SchemaVersion != 0 {
			return fmt.Errorf("%w: zero checkpoint", ErrSpaceChangefeedInvalid)
		}
		return nil
	}
	if checkpoint.SchemaVersion != feed.schemaVersion {
		return fmt.Errorf("%w: checkpoint schema version", ErrSpaceChangefeedInvalid)
	}
	if checkpoint.Sequence > feed.latest.Sequence || checkpoint.Frontier > feed.latest.Frontier {
		return fmt.Errorf("%w: checkpoint is ahead", ErrSpaceChangefeedSequence)
	}
	if !allowExpired && len(feed.history) > 0 {
		earliest := feed.history[0].sequence
		if checkpoint.Sequence < earliest && earliest-checkpoint.Sequence > 1 {
			return ErrSpaceChangefeedCheckpointExpired
		}
	}
	return nil
}

func (feed *SpaceChangefeed) replayAfterLocked(sequence uint64) []spaceChangefeedStoredBatch {
	if len(feed.history) == 0 {
		return nil
	}
	start := 0
	for start < len(feed.history) && feed.history[start].sequence <= sequence {
		start++
	}
	return feed.history[start:]
}

func (feed *SpaceChangefeed) closeSubscriberLocked(subscriber *spaceChangefeedSubscriber, err error) {
	if subscriber == nil || subscriber.closed {
		return
	}
	subscriber.closed = true
	subscriber.err = err
	close(subscriber.done)
	close(subscriber.updates)
}

func storeSpaceChangefeedBatch(batch SpaceChangefeedBatch, payloadBytes int) spaceChangefeedStoredBatch {
	stored := spaceChangefeedStoredBatch{
		sequence:      batch.Sequence,
		frontier:      batch.Frontier,
		schemaVersion: batch.SchemaVersion,
		changes:       make([]spaceChangefeedStoredChange, len(batch.Changes)),
		payload:       make([]byte, payloadBytes),
		snapshot:      batch.Snapshot,
		progress:      batch.Progress,
		complete:      batch.Complete,
	}
	offset := 0
	for index, change := range batch.Changes {
		storedChange := spaceChangefeedStoredChange{operation: change.Operation}
		storedChange.keyStart, storedChange.keyLength, offset = copySpaceChangefeedPayload(stored.payload, offset, change.Key)
		storedChange.beforeStart, storedChange.beforeLen, offset = copySpaceChangefeedPayload(stored.payload, offset, change.Before)
		storedChange.afterStart, storedChange.afterLen, offset = copySpaceChangefeedPayload(stored.payload, offset, change.After)
		stored.changes[index] = storedChange
	}
	return stored
}

func copySpaceChangefeedPayload(destination []byte, offset int, source []byte) (start, length, next int) {
	start = offset
	length = len(source)
	copy(destination[offset:offset+len(source)], source)
	return start, length, offset + len(source)
}

func cloneStoredSpaceChangefeedBatch(stored spaceChangefeedStoredBatch) SpaceChangefeedBatch {
	cloned := SpaceChangefeedBatch{
		Sequence:      stored.sequence,
		Frontier:      stored.frontier,
		SchemaVersion: stored.schemaVersion,
		Snapshot:      stored.snapshot,
		Progress:      stored.progress,
		Complete:      stored.complete,
	}
	if len(stored.changes) == 0 {
		return cloned
	}
	cloned.Changes = make([]SpaceChange, len(stored.changes))
	payload := append([]byte(nil), stored.payload...)
	for index, change := range stored.changes {
		cloned.Changes[index] = SpaceChange{
			Operation: change.operation,
			Key:       payload[change.keyStart : change.keyStart+change.keyLength],
			Before:    payload[change.beforeStart : change.beforeStart+change.beforeLen],
			After:     payload[change.afterStart : change.afterStart+change.afterLen],
		}
	}
	return cloned
}
