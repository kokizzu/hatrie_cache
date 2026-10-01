package hatReplication

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	// ErrSpaceChangefeedInvalid indicates a nil feed or subscription.
	ErrSpaceChangefeedInvalid = errors.New("hatReplication: space changefeed is invalid")
	// ErrSpaceChangefeedSpaceRequired indicates a missing or invalid space name.
	ErrSpaceChangefeedSpaceRequired = errors.New("hatReplication: space changefeed space is required")
	// ErrSpaceChangefeedSchemaInvalid indicates an unsupported schema version.
	ErrSpaceChangefeedSchemaInvalid = errors.New("hatReplication: space changefeed schema version is invalid")
	// ErrSpaceChangefeedCapacityInvalid indicates an unsupported event capacity.
	ErrSpaceChangefeedCapacityInvalid = errors.New("hatReplication: space changefeed capacity is invalid")
	// ErrSpaceChangefeedSchemaMismatch indicates a checkpoint for another schema.
	ErrSpaceChangefeedSchemaMismatch = errors.New("hatReplication: space changefeed schema version mismatch")
	// ErrSpaceChangefeedCheckpointAhead indicates a checkpoint newer than the feed.
	ErrSpaceChangefeedCheckpointAhead = errors.New("hatReplication: space changefeed checkpoint is ahead")
	// ErrSpaceChangefeedHistoryGone indicates that retention no longer covers a checkpoint.
	ErrSpaceChangefeedHistoryGone = errors.New("hatReplication: space changefeed history is no longer retained")
	// ErrSpaceChangefeedBatchTooLarge indicates a batch larger than the bounded log.
	ErrSpaceChangefeedBatchTooLarge = errors.New("hatReplication: space changefeed batch exceeds capacity")
	// ErrSpaceChangefeedClosed indicates that the feed has been closed.
	ErrSpaceChangefeedClosed = errors.New("hatReplication: space changefeed is closed")
	// ErrSpaceChangefeedSubscriptionClosed indicates that a subscription has been closed.
	ErrSpaceChangefeedSubscriptionClosed = errors.New("hatReplication: space changefeed subscription is closed")
	// ErrSpaceChangefeedAckInvalid indicates an acknowledgement beyond delivery.
	ErrSpaceChangefeedAckInvalid = errors.New("hatReplication: space changefeed acknowledgement is invalid")
	// ErrSpaceChangefeedOperationInvalid indicates an unsupported change operation.
	ErrSpaceChangefeedOperationInvalid = errors.New("hatReplication: space changefeed operation is invalid")
	// ErrSpaceChangefeedPayloadTooLarge indicates an event payload above the
	// bounded per-event limit.
	ErrSpaceChangefeedPayloadTooLarge = errors.New("hatReplication: space changefeed payload is too large")
	// ErrSpaceChangefeedSequenceExhausted indicates that the sequence space is exhausted.
	ErrSpaceChangefeedSequenceExhausted = errors.New("hatReplication: space changefeed sequence is exhausted")
)

const (
	// DefaultSpaceChangefeedCapacity is the bounded retained-event default.
	DefaultSpaceChangefeedCapacity = 1024
	// MaxSpaceChangefeedCapacity prevents an accidental unbounded replay log.
	MaxSpaceChangefeedCapacity = 1 << 16
	// MaxSpaceChangefeedSpaceBytes bounds the retained space identity.
	MaxSpaceChangefeedSpaceBytes = 256
	// MaxSpaceChangefeedChangeBytes bounds one packed change payload.
	MaxSpaceChangefeedChangeBytes = 16 << 20
)

// SpaceChangefeedOperation identifies the row transition in one event. The
// payload bytes are intentionally opaque so SQL and non-SQL serializers can
// share the same bounded transport primitive.
type SpaceChangefeedOperation uint8

const (
	SpaceChangefeedInvalid SpaceChangefeedOperation = iota
	SpaceChangefeedCreate
	SpaceChangefeedUpdate
	SpaceChangefeedDelete
	SpaceChangefeedRead
)

// String returns the stable operation name.
func (operation SpaceChangefeedOperation) String() string {
	switch operation {
	case SpaceChangefeedCreate:
		return "create"
	case SpaceChangefeedUpdate:
		return "update"
	case SpaceChangefeedDelete:
		return "delete"
	case SpaceChangefeedRead:
		return "read"
	default:
		return "invalid"
	}
}

// SpaceChangefeedChange contains copied opaque key and row images. Before is
// used for updates/deletes and After is used for creates/updates/reads.
type SpaceChangefeedChange struct {
	Key       []byte                   `json:"key,omitempty"`
	Before    []byte                   `json:"before,omitempty"`
	After     []byte                   `json:"after,omitempty"`
	Operation SpaceChangefeedOperation `json:"operation"`
}

// SpaceChangefeedOptions configures one named-space replay log. Capacity zero
// uses DefaultSpaceChangefeedCapacity. SchemaVersion must be nonzero.
type SpaceChangefeedOptions struct {
	Space         string
	SchemaVersion uint64
	Capacity      int
}

// SpaceChangefeedCheckpoint identifies an acknowledged or published position
// for one space schema. It is safe to persist and pass back to Subscribe.
type SpaceChangefeedCheckpoint struct {
	Space         string `json:"space"`
	SchemaVersion uint64 `json:"schema_version"`
	Sequence      uint64 `json:"sequence"`
}

// SpaceChangefeedEvent is one ordered event delivered to a subscription.
type SpaceChangefeedEvent struct {
	Checkpoint SpaceChangefeedCheckpoint `json:"checkpoint"`
	Change     SpaceChangefeedChange     `json:"change"`
}

type spaceChangefeedStoredChange struct {
	payload                []byte
	keyStart, keyEnd       int
	beforeStart, beforeEnd int
	afterStart, afterEnd   int
	operation              SpaceChangefeedOperation
}

type spaceChangefeedStoredEvent struct {
	checkpoint SpaceChangefeedCheckpoint
	change     spaceChangefeedStoredChange
}

// SpaceChangefeed is a bounded replay log for one named space. Producers wait
// for acknowledged capacity with context cancellation; subscribers advance a
// private delivery cursor and explicitly acknowledge durable progress.
type SpaceChangefeed struct {
	mu             sync.Mutex
	space          string
	schemaVersion  uint64
	capacity       int
	events         []spaceChangefeedStoredEvent
	firstSequence  uint64
	nextSequence   uint64
	nextSubID      uint64
	subscribers    map[uint64]*SpaceChangefeedSubscription
	cond           *sync.Cond
	notify         chan struct{}
	contextWaiters int
	closed         bool
}

// SpaceChangefeedSubscription is a cursor over one feed. It does not start a
// goroutine; Next waits on the feed's shared notification channel.
type SpaceChangefeedSubscription struct {
	feed   *SpaceChangefeed
	id     uint64
	cursor uint64
	acked  uint64
	closed bool
}

// NewSpaceChangefeed creates an empty bounded replay log.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space := strings.TrimSpace(options.Space)
	if space == "" || len(space) > MaxSpaceChangefeedSpaceBytes || !utf8.ValidString(space) {
		return nil, ErrSpaceChangefeedSpaceRequired
	}
	if options.SchemaVersion == 0 {
		return nil, ErrSpaceChangefeedSchemaInvalid
	}
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultSpaceChangefeedCapacity
	}
	if capacity < 1 || capacity > MaxSpaceChangefeedCapacity {
		return nil, ErrSpaceChangefeedCapacityInvalid
	}
	feed := &SpaceChangefeed{
		space:         space,
		schemaVersion: options.SchemaVersion,
		capacity:      capacity,
		firstSequence: 1,
		nextSequence:  1,
		subscribers:   make(map[uint64]*SpaceChangefeedSubscription),
		notify:        make(chan struct{}),
	}
	feed.cond = sync.NewCond(&feed.mu)
	return feed, nil
}

// InitialCheckpoint returns the zero position for this feed.
func (feed *SpaceChangefeed) InitialCheckpoint() SpaceChangefeedCheckpoint {
	if feed == nil || feed.cond == nil {
		return SpaceChangefeedCheckpoint{}
	}
	return SpaceChangefeedCheckpoint{Space: feed.space, SchemaVersion: feed.schemaVersion}
}

// CurrentCheckpoint returns the newest published sequence.
func (feed *SpaceChangefeed) CurrentCheckpoint() SpaceChangefeedCheckpoint {
	if feed == nil || feed.cond == nil {
		return SpaceChangefeedCheckpoint{}
	}
	feed.mu.Lock()
	checkpoint := feed.currentCheckpointLocked()
	feed.mu.Unlock()
	return checkpoint
}

// Subscribe opens a cursor after checkpoint. The checkpoint must match the
// feed's space and schema and must still be within retained history.
func (feed *SpaceChangefeed) Subscribe(checkpoint SpaceChangefeedCheckpoint) (*SpaceChangefeedSubscription, error) {
	if feed == nil || feed.cond == nil {
		return nil, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if strings.TrimSpace(checkpoint.Space) != feed.space || checkpoint.SchemaVersion != feed.schemaVersion {
		return nil, ErrSpaceChangefeedSchemaMismatch
	}
	if checkpoint.Sequence > feed.lastSequenceLocked() {
		return nil, ErrSpaceChangefeedCheckpointAhead
	}
	if checkpoint.Sequence < feed.firstSequence-1 {
		return nil, ErrSpaceChangefeedHistoryGone
	}
	feed.nextSubID++
	subscription := &SpaceChangefeedSubscription{
		feed:   feed,
		id:     feed.nextSubID,
		cursor: checkpoint.Sequence,
		acked:  checkpoint.Sequence,
	}
	feed.subscribers[subscription.id] = subscription
	return subscription, nil
}

// Publish appends one atomically visible batch. When active subscribers have
// not acknowledged enough history, it waits for acknowledgement or ctx
// cancellation. Payload slices are copied before publication.
func (feed *SpaceChangefeed) Publish(ctx context.Context, changes []SpaceChangefeedChange) (SpaceChangefeedCheckpoint, error) {
	if feed == nil || feed.cond == nil {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SpaceChangefeedCheckpoint{}, err
	}
	if len(changes) > feed.capacity {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedBatchTooLarge
	}
	for _, change := range changes {
		if change.Operation < SpaceChangefeedCreate || change.Operation > SpaceChangefeedRead {
			return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedOperationInvalid
		}
		if spaceChangefeedPayloadLength(change) > MaxSpaceChangefeedChangeBytes {
			return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedPayloadTooLarge
		}
	}

	feed.mu.Lock()
	for {
		if feed.closed {
			feed.mu.Unlock()
			return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedClosed
		}
		if err := ctx.Err(); err != nil {
			feed.mu.Unlock()
			return SpaceChangefeedCheckpoint{}, err
		}
		if len(feed.subscribers) == 0 {
			for len(feed.events)+len(changes) > feed.capacity {
				feed.dropOldestLocked()
			}
			break
		}
		feed.trimAcknowledgedLocked()
		if len(feed.events)+len(changes) <= feed.capacity {
			break
		}
		if ctx.Done() == nil {
			feed.cond.Wait()
			continue
		}
		wait := feed.notify
		feed.contextWaiters++
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-wait:
		}
		feed.mu.Lock()
		feed.contextWaiters--
		if err := ctx.Err(); err != nil {
			feed.mu.Unlock()
			return SpaceChangefeedCheckpoint{}, err
		}
	}

	if len(changes) > 0 && uint64(len(changes)-1) > ^uint64(0)-feed.nextSequence {
		feed.mu.Unlock()
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedSequenceExhausted
	}
	if len(feed.events) == 0 {
		feed.firstSequence = feed.nextSequence
	}
	for _, change := range changes {
		sequence := feed.nextSequence
		feed.nextSequence++
		feed.events = append(feed.events, spaceChangefeedStoredEvent{
			checkpoint: SpaceChangefeedCheckpoint{Space: feed.space, SchemaVersion: feed.schemaVersion, Sequence: sequence},
			change:     packSpaceChangefeedChange(change),
		})
	}
	checkpoint := feed.currentCheckpointLocked()
	feed.signalLocked()
	feed.mu.Unlock()
	return checkpoint, nil
}

// Close stops future publication and wakes all waiting subscribers and
// producers. Retained events remain readable until each subscription closes.
func (feed *SpaceChangefeed) Close() error {
	if feed == nil || feed.cond == nil {
		return ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	if !feed.closed {
		feed.closed = true
		feed.signalLocked()
	}
	feed.mu.Unlock()
	return nil
}

// Next returns the next event after the subscription cursor, waiting until an
// event, feed close, or context cancellation is available.
func (subscription *SpaceChangefeedSubscription) Next(ctx context.Context) (SpaceChangefeedEvent, error) {
	if subscription == nil || subscription.feed == nil {
		return SpaceChangefeedEvent{}, ErrSpaceChangefeedInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	feed := subscription.feed
	for {
		if err := ctx.Err(); err != nil {
			return SpaceChangefeedEvent{}, err
		}
		feed.mu.Lock()
		if subscription.closed {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, ErrSpaceChangefeedSubscriptionClosed
		}
		if subscription.cursor < feed.firstSequence-1 {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, ErrSpaceChangefeedHistoryGone
		}
		next := subscription.cursor + 1
		if next <= feed.lastSequenceLocked() {
			if len(feed.events) == 0 || next < feed.firstSequence {
				feed.mu.Unlock()
				return SpaceChangefeedEvent{}, ErrSpaceChangefeedHistoryGone
			}
			event := feed.events[int(next-feed.firstSequence)]
			subscription.cursor = next
			feed.mu.Unlock()
			return cloneSpaceChangefeedEvent(event), nil
		}
		if feed.closed {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, ErrSpaceChangefeedClosed
		}
		if ctx.Done() == nil {
			feed.cond.Wait()
			continue
		}
		wait := feed.notify
		feed.contextWaiters++
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-wait:
		}
		feed.mu.Lock()
		feed.contextWaiters--
		if err := ctx.Err(); err != nil {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, err
		}
	}
}

// Ack durably advances the subscription checkpoint. Acknowledgements are
// idempotent and release retained capacity once every subscriber has passed
// the same event.
func (subscription *SpaceChangefeedSubscription) Ack(sequence uint64) error {
	if subscription == nil || subscription.feed == nil {
		return ErrSpaceChangefeedInvalid
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.closed {
		return ErrSpaceChangefeedSubscriptionClosed
	}
	if sequence > subscription.cursor {
		return ErrSpaceChangefeedAckInvalid
	}
	if sequence <= subscription.acked {
		return nil
	}
	subscription.acked = sequence
	feed.trimAcknowledgedLocked()
	feed.signalLocked()
	return nil
}

// Checkpoint returns the last acknowledged position for this subscription.
func (subscription *SpaceChangefeedSubscription) Checkpoint() SpaceChangefeedCheckpoint {
	if subscription == nil || subscription.feed == nil {
		return SpaceChangefeedCheckpoint{}
	}
	feed := subscription.feed
	feed.mu.Lock()
	checkpoint := SpaceChangefeedCheckpoint{Space: feed.space, SchemaVersion: feed.schemaVersion, Sequence: subscription.acked}
	feed.mu.Unlock()
	return checkpoint
}

// Close detaches the subscription and releases its retention obligation.
func (subscription *SpaceChangefeedSubscription) Close() error {
	if subscription == nil || subscription.feed == nil {
		return ErrSpaceChangefeedInvalid
	}
	feed := subscription.feed
	feed.mu.Lock()
	if !subscription.closed {
		subscription.closed = true
		delete(feed.subscribers, subscription.id)
		feed.trimAcknowledgedLocked()
		feed.signalLocked()
	}
	feed.mu.Unlock()
	return nil
}

func (feed *SpaceChangefeed) currentCheckpointLocked() SpaceChangefeedCheckpoint {
	return SpaceChangefeedCheckpoint{Space: feed.space, SchemaVersion: feed.schemaVersion, Sequence: feed.lastSequenceLocked()}
}

func (feed *SpaceChangefeed) lastSequenceLocked() uint64 {
	if feed.nextSequence == 0 {
		return 0
	}
	return feed.nextSequence - 1
}

func (feed *SpaceChangefeed) trimAcknowledgedLocked() {
	if len(feed.subscribers) == 0 {
		return
	}
	minimum := feed.lastSequenceLocked()
	for _, subscription := range feed.subscribers {
		if subscription.acked < minimum {
			minimum = subscription.acked
		}
	}
	for len(feed.events) > 0 && feed.events[0].checkpoint.Sequence <= minimum {
		feed.dropOldestLocked()
	}
}

func (feed *SpaceChangefeed) dropOldestLocked() {
	if len(feed.events) == 0 {
		return
	}
	feed.events[0] = spaceChangefeedStoredEvent{}
	feed.events = feed.events[1:]
	feed.firstSequence++
	if len(feed.events) == 0 {
		feed.firstSequence = feed.nextSequence
	}
}

func (feed *SpaceChangefeed) signalLocked() {
	if feed.cond != nil {
		feed.cond.Broadcast()
	}
	if feed.contextWaiters > 0 {
		close(feed.notify)
		feed.notify = make(chan struct{})
	}
}

func cloneSpaceChangefeedEvent(event spaceChangefeedStoredEvent) SpaceChangefeedEvent {
	return SpaceChangefeedEvent{Checkpoint: event.checkpoint, Change: cloneSpaceChangefeedStoredChange(event.change)}
}

func packSpaceChangefeedChange(change SpaceChangefeedChange) spaceChangefeedStoredChange {
	keyLength := len(change.Key)
	beforeLength := len(change.Before)
	afterLength := len(change.After)
	payload := make([]byte, keyLength+beforeLength+afterLength)
	keyStart := 0
	keyEnd := keyStart + keyLength
	beforeStart := keyEnd
	beforeEnd := beforeStart + beforeLength
	afterStart := beforeEnd
	afterEnd := afterStart + afterLength
	copy(payload[keyStart:keyEnd], change.Key)
	copy(payload[beforeStart:beforeEnd], change.Before)
	copy(payload[afterStart:afterEnd], change.After)
	return spaceChangefeedStoredChange{
		payload:     payload,
		keyStart:    keyStart,
		keyEnd:      keyEnd,
		beforeStart: beforeStart,
		beforeEnd:   beforeEnd,
		afterStart:  afterStart,
		afterEnd:    afterEnd,
		operation:   change.Operation,
	}
}

func cloneSpaceChangefeedStoredChange(stored spaceChangefeedStoredChange) SpaceChangefeedChange {
	payload := append([]byte(nil), stored.payload...)
	return SpaceChangefeedChange{
		Key:       payload[stored.keyStart:stored.keyEnd],
		Before:    payload[stored.beforeStart:stored.beforeEnd],
		After:     payload[stored.afterStart:stored.afterEnd],
		Operation: stored.operation,
	}
}

func spaceChangefeedPayloadLength(change SpaceChangefeedChange) int {
	length := len(change.Key)
	if len(change.Before) > MaxSpaceChangefeedChangeBytes-length {
		return MaxSpaceChangefeedChangeBytes + 1
	}
	length += len(change.Before)
	if len(change.After) > MaxSpaceChangefeedChangeBytes-length {
		return MaxSpaceChangefeedChangeBytes + 1
	}
	return length + len(change.After)
}
