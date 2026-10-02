package hatReplication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSpaceChangefeedCapacity bounds retained replay history.
	DefaultSpaceChangefeedCapacity = 1024
	// MaxSpaceChangefeedCapacity prevents an accidental unbounded history.
	MaxSpaceChangefeedCapacity = 65536
	// DefaultSpaceChangefeedSubscriberBuffer is used when Subscribe receives zero.
	DefaultSpaceChangefeedSubscriberBuffer = 128
	// MaxSpaceChangefeedSubscriberBuffer bounds one subscriber queue.
	MaxSpaceChangefeedSubscriberBuffer = 4096
	// MaxSpaceChangefeedOperationBytes bounds operation names.
	MaxSpaceChangefeedOperationBytes = 128
	// MaxSpaceChangefeedKeyBytes bounds one event key.
	MaxSpaceChangefeedKeyBytes = 1 << 20
	// MaxSpaceChangefeedValueBytes bounds one event value.
	MaxSpaceChangefeedValueBytes = 16 << 20
	// MaxSpaceChangefeedSpaceBytes bounds the named space and checkpoint source.
	MaxSpaceChangefeedSpaceBytes = 256

	spaceChangefeedCheckpointHeaderBytes  = 4 + 2 + 8 + 8
	spaceChangefeedCheckpointSourceOffset = spaceChangefeedCheckpointHeaderBytes
	MaxSpaceChangefeedCheckpointBytes     = spaceChangefeedCheckpointHeaderBytes + MaxSpaceChangefeedSpaceBytes + 4
)

var (
	// ErrSpaceChangefeedInvalid indicates invalid feed, event, or option input.
	ErrSpaceChangefeedInvalid = errors.New("hatriecache: space changefeed is invalid")
	// ErrSpaceChangefeedClosed indicates a feed that no longer accepts events.
	ErrSpaceChangefeedClosed = errors.New("hatriecache: space changefeed is closed")
	// ErrSpaceChangefeedSubscriptionClosed indicates a closed subscriber.
	ErrSpaceChangefeedSubscriptionClosed = errors.New("hatriecache: space changefeed subscription is closed")
	// ErrSpaceChangefeedCheckpointInvalid indicates a checkpoint for another feed.
	ErrSpaceChangefeedCheckpointInvalid = errors.New("hatriecache: space changefeed checkpoint is invalid")
	// ErrSpaceChangefeedCheckpointExpired indicates history was evicted.
	ErrSpaceChangefeedCheckpointExpired = errors.New("hatriecache: space changefeed checkpoint expired")
	// ErrSpaceChangefeedBufferTooSmall indicates replay cannot fit losslessly.
	ErrSpaceChangefeedBufferTooSmall = errors.New("hatriecache: space changefeed subscriber buffer is too small")
	// ErrSpaceChangefeedSchemaMismatch indicates an event from another schema.
	ErrSpaceChangefeedSchemaMismatch = errors.New("hatriecache: space changefeed schema version mismatch")

	spaceChangefeedCheckpointCRC = crc32.MakeTable(crc32.Castagnoli)
)

var spaceChangefeedCheckpointMagic = [4]byte{'s', 'c', 'f', '1'}

// SpaceChangefeedEvent is one immutable-by-convention named-space change. The
// feed copies Key and Value before publishing, and readers receive new copies.
type SpaceChangefeedEvent struct {
	Sequence      uint64    `json:"sequence"`
	Space         string    `json:"space"`
	SchemaVersion uint64    `json:"schema_version"`
	Operation     string    `json:"operation"`
	Key           []byte    `json:"key"`
	Value         []byte    `json:"value,omitempty"`
	At            time.Time `json:"at"`
}

// SpaceChangefeedCheckpoint identifies a consumer's durable position and the
// schema it consumed. It is safe to persist and restore as a bounded frame.
type SpaceChangefeedCheckpoint struct {
	Space         string `json:"space"`
	SchemaVersion uint64 `json:"schema_version"`
	Sequence      uint64 `json:"sequence"`
}

// NewSpaceChangefeedCheckpoint creates a validated checkpoint.
func NewSpaceChangefeedCheckpoint(space string, schemaVersion, sequence uint64) (SpaceChangefeedCheckpoint, error) {
	space = strings.TrimSpace(space)
	if space == "" || len(space) > MaxSpaceChangefeedSpaceBytes || schemaVersion == 0 {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	return SpaceChangefeedCheckpoint{Space: space, SchemaVersion: schemaVersion, Sequence: sequence}, nil
}

// Advance returns a checkpoint at sequence. Regressions are rejected.
func (checkpoint SpaceChangefeedCheckpoint) Advance(sequence uint64) (SpaceChangefeedCheckpoint, error) {
	validated, err := NewSpaceChangefeedCheckpoint(checkpoint.Space, checkpoint.SchemaVersion, checkpoint.Sequence)
	if err != nil {
		return SpaceChangefeedCheckpoint{}, err
	}
	if sequence < validated.Sequence {
		return SpaceChangefeedCheckpoint{}, fmt.Errorf("%w: current=%d requested=%d", ErrSpaceChangefeedCheckpointInvalid, validated.Sequence, sequence)
	}
	validated.Sequence = sequence
	return validated, nil
}

// MarshalBinary encodes a deterministic, CRC32C-protected checkpoint.
func (checkpoint SpaceChangefeedCheckpoint) MarshalBinary() ([]byte, error) {
	validated, err := NewSpaceChangefeedCheckpoint(checkpoint.Space, checkpoint.SchemaVersion, checkpoint.Sequence)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, spaceChangefeedCheckpointHeaderBytes+len(validated.Space)+4)
	copy(encoded, spaceChangefeedCheckpointMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], uint16(len(validated.Space)))
	binary.BigEndian.PutUint64(encoded[6:14], validated.SchemaVersion)
	binary.BigEndian.PutUint64(encoded[14:22], validated.Sequence)
	copy(encoded[spaceChangefeedCheckpointSourceOffset:], validated.Space)
	checksumOffset := len(encoded) - 4
	binary.BigEndian.PutUint32(encoded[checksumOffset:], crc32.Checksum(encoded[:checksumOffset], spaceChangefeedCheckpointCRC))
	return encoded, nil
}

// UnmarshalSpaceChangefeedCheckpoint validates and owns a decoded checkpoint.
func UnmarshalSpaceChangefeedCheckpoint(encoded []byte) (SpaceChangefeedCheckpoint, error) {
	if len(encoded) < spaceChangefeedCheckpointHeaderBytes+4 || len(encoded) > MaxSpaceChangefeedCheckpointBytes {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	if string(encoded[:len(spaceChangefeedCheckpointMagic)]) != string(spaceChangefeedCheckpointMagic[:]) {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	spaceLength := int(binary.BigEndian.Uint16(encoded[4:6]))
	expectedLength := spaceChangefeedCheckpointHeaderBytes + spaceLength + 4
	if spaceLength == 0 || spaceLength > MaxSpaceChangefeedSpaceBytes || len(encoded) != expectedLength {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	checksumOffset := len(encoded) - 4
	if binary.BigEndian.Uint32(encoded[checksumOffset:]) != crc32.Checksum(encoded[:checksumOffset], spaceChangefeedCheckpointCRC) {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	space := string(encoded[spaceChangefeedCheckpointSourceOffset:checksumOffset])
	if strings.TrimSpace(space) != space {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	checkpoint, err := NewSpaceChangefeedCheckpoint(space, binary.BigEndian.Uint64(encoded[6:14]), binary.BigEndian.Uint64(encoded[14:22]))
	if err != nil {
		return SpaceChangefeedCheckpoint{}, err
	}
	return checkpoint, nil
}

// SpaceChangefeedOptions configures one named-space feed. A zero Capacity
// uses the default retained history, and a nil Now uses time.Now.
type SpaceChangefeedOptions struct {
	Space         string
	SchemaVersion uint64
	Capacity      int
	Now           func() time.Time
}

// SpaceChangefeed is a bounded replay history with lossless, context-aware
// subscriber backpressure. It is opt-in and independent of legacy ChangeLog.
type SpaceChangefeed struct {
	mu             sync.Mutex
	space          string
	schemaVersion  uint64
	capacity       int
	now            func() time.Time
	history        []SpaceChangefeedEvent
	start          int
	count          int
	next           uint64
	subscribers    map[uint64]*SpaceChangefeedSubscription
	nextSubscriber uint64
	changed        chan struct{}
	closed         bool
}

// NewSpaceChangefeed creates a bounded named-space feed.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space := strings.TrimSpace(options.Space)
	if space == "" || len(space) > MaxSpaceChangefeedSpaceBytes || options.SchemaVersion == 0 {
		return nil, ErrSpaceChangefeedInvalid
	}
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultSpaceChangefeedCapacity
	}
	if capacity < 1 || capacity > MaxSpaceChangefeedCapacity {
		return nil, ErrSpaceChangefeedInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &SpaceChangefeed{
		space:         space,
		schemaVersion: options.SchemaVersion,
		capacity:      capacity,
		now:           now,
		history:       make([]SpaceChangefeedEvent, capacity),
		subscribers:   make(map[uint64]*SpaceChangefeedSubscription),
		changed:       make(chan struct{}),
	}, nil
}

// Publish appends an event. If any subscriber queue is full, it waits for
// that subscriber to receive an event or for ctx to be canceled.
func (feed *SpaceChangefeed) Publish(ctx context.Context, event SpaceChangefeedEvent) (SpaceChangefeedEvent, error) {
	if feed == nil {
		return SpaceChangefeedEvent{}, ErrSpaceChangefeedInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SpaceChangefeedEvent{}, err
	}
	for {
		feed.mu.Lock()
		if feed.closed {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, ErrSpaceChangefeedClosed
		}
		if err := feed.validateEvent(event); err != nil {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, err
		}
		if feed.canPublishLocked() {
			if feed.next == ^uint64(0) {
				feed.mu.Unlock()
				return SpaceChangefeedEvent{}, ErrSpaceChangefeedInvalid
			}
			feed.next++
			stored := cloneSpaceChangefeedEvent(event)
			stored.Sequence = feed.next
			stored.Space = feed.space
			stored.SchemaVersion = feed.schemaVersion
			if stored.At.IsZero() {
				stored.At = feed.now().UTC()
			} else {
				stored.At = stored.At.UTC()
			}
			feed.appendHistoryLocked(stored)
			for _, subscription := range feed.subscribers {
				subscription.enqueueLocked(stored)
				subscription.signalLocked()
			}
			feed.signalLocked()
			feed.mu.Unlock()
			return cloneSpaceChangefeedEvent(stored), nil
		}
		wait := feed.changed
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
			return SpaceChangefeedEvent{}, ctx.Err()
		case <-wait:
		}
	}
}

// Read returns retained events newer than checkpoint without registering a
// subscriber. It is useful for stateless pull consumers.
func (feed *SpaceChangefeed) Read(checkpoint SpaceChangefeedCheckpoint, limit int) ([]SpaceChangefeedEvent, SpaceChangefeedCheckpoint, error) {
	if feed == nil || limit < 1 || limit > MaxSpaceChangefeedSubscriberBuffer {
		return nil, checkpoint, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if err := feed.validateCheckpointLocked(checkpoint); err != nil {
		return nil, checkpoint, err
	}
	if checkpoint.Sequence == feed.next || feed.count == 0 {
		return nil, checkpoint, nil
	}
	oldest := feed.oldestSequenceLocked()
	sequence := checkpoint.Sequence + 1
	if sequence < oldest {
		sequence = oldest
	}
	count := int(feed.next - sequence + 1)
	if count > limit {
		count = limit
	}
	events := make([]SpaceChangefeedEvent, count)
	for index := range events {
		events[index] = cloneSpaceChangefeedEvent(feed.historyAtLocked(sequence + uint64(index)))
	}
	next, err := checkpoint.Advance(events[len(events)-1].Sequence)
	if err != nil {
		return nil, checkpoint, err
	}
	return events, next, nil
}

// Subscribe registers a bounded queue and replays retained history after the
// checkpoint. A buffer too small for replay is rejected rather than dropping data.
func (feed *SpaceChangefeed) Subscribe(checkpoint SpaceChangefeedCheckpoint, buffer int) (*SpaceChangefeedSubscription, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedInvalid
	}
	if buffer == 0 {
		buffer = DefaultSpaceChangefeedSubscriberBuffer
	}
	if buffer < 1 || buffer > MaxSpaceChangefeedSubscriberBuffer {
		return nil, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	if err := feed.validateCheckpointLocked(checkpoint); err != nil {
		return nil, err
	}
	pending := int(feed.next - checkpoint.Sequence)
	if pending > buffer {
		return nil, fmt.Errorf("%w: pending=%d buffer=%d", ErrSpaceChangefeedBufferTooSmall, pending, buffer)
	}
	feed.nextSubscriber++
	subscription := &SpaceChangefeedSubscription{
		feed:       feed,
		id:         feed.nextSubscriber,
		buffer:     make([]SpaceChangefeedEvent, buffer),
		capacity:   buffer,
		checkpoint: checkpoint,
		changed:    make(chan struct{}),
	}
	if pending > 0 {
		oldest := feed.oldestSequenceLocked()
		for index := 0; index < pending; index++ {
			subscription.enqueueLocked(feed.historyAtLocked(oldest + uint64(index)))
		}
	}
	feed.subscribers[subscription.id] = subscription
	return subscription, nil
}

// Close stops the feed and wakes all blocked publishers and subscribers.
func (feed *SpaceChangefeed) Close() error {
	if feed == nil {
		return ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil
	}
	feed.closed = true
	for _, subscription := range feed.subscribers {
		subscription.closed = true
		subscription.signalLocked()
	}
	feed.subscribers = nil
	feed.signalLocked()
	return nil
}

// SpaceChangefeedSubscription is one bounded consumer queue.
type SpaceChangefeedSubscription struct {
	feed       *SpaceChangefeed
	id         uint64
	buffer     []SpaceChangefeedEvent
	start      int
	count      int
	capacity   int
	checkpoint SpaceChangefeedCheckpoint
	changed    chan struct{}
	closed     bool
}

// Receive waits for and removes one event from the subscription queue.
func (subscription *SpaceChangefeedSubscription) Receive(ctx context.Context) (SpaceChangefeedEvent, error) {
	if subscription == nil || subscription.feed == nil {
		return SpaceChangefeedEvent{}, ErrSpaceChangefeedSubscriptionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		feed := subscription.feed
		feed.mu.Lock()
		if subscription.closed {
			feed.mu.Unlock()
			return SpaceChangefeedEvent{}, ErrSpaceChangefeedSubscriptionClosed
		}
		if subscription.count > 0 {
			event := subscription.dequeueLocked()
			checkpoint, err := subscription.checkpoint.Advance(event.Sequence)
			if err != nil {
				feed.mu.Unlock()
				return SpaceChangefeedEvent{}, err
			}
			subscription.checkpoint = checkpoint
			feed.signalLocked()
			feed.mu.Unlock()
			return cloneSpaceChangefeedEvent(event), nil
		}
		wait := subscription.changed
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
			return SpaceChangefeedEvent{}, ctx.Err()
		case <-wait:
		}
	}
}

// Checkpoint returns the last consumed position.
func (subscription *SpaceChangefeedSubscription) Checkpoint() SpaceChangefeedCheckpoint {
	if subscription == nil || subscription.feed == nil {
		return SpaceChangefeedCheckpoint{}
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return subscription.checkpoint
}

// Close removes the subscription and releases publisher backpressure.
func (subscription *SpaceChangefeedSubscription) Close() error {
	if subscription == nil || subscription.feed == nil {
		return ErrSpaceChangefeedSubscriptionClosed
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.closed {
		return nil
	}
	subscription.closed = true
	delete(feed.subscribers, subscription.id)
	subscription.signalLocked()
	feed.signalLocked()
	return nil
}

func (feed *SpaceChangefeed) validateEvent(event SpaceChangefeedEvent) error {
	if event.Space != feed.space {
		return ErrSpaceChangefeedInvalid
	}
	if event.SchemaVersion != feed.schemaVersion {
		return ErrSpaceChangefeedSchemaMismatch
	}
	if strings.TrimSpace(event.Operation) == "" || len(event.Operation) > MaxSpaceChangefeedOperationBytes || len(event.Key) == 0 || len(event.Key) > MaxSpaceChangefeedKeyBytes || len(event.Value) > MaxSpaceChangefeedValueBytes {
		return ErrSpaceChangefeedInvalid
	}
	return nil
}

func (feed *SpaceChangefeed) validateCheckpointLocked(checkpoint SpaceChangefeedCheckpoint) error {
	if checkpoint.Space != feed.space || checkpoint.SchemaVersion != feed.schemaVersion || checkpoint.Space == "" || checkpoint.SchemaVersion == 0 {
		return ErrSpaceChangefeedCheckpointInvalid
	}
	if checkpoint.Sequence > feed.next {
		return ErrSpaceChangefeedCheckpointInvalid
	}
	if feed.count > 0 && checkpoint.Sequence < feed.oldestSequenceLocked()-1 {
		return ErrSpaceChangefeedCheckpointExpired
	}
	return nil
}

func (feed *SpaceChangefeed) canPublishLocked() bool {
	for _, subscription := range feed.subscribers {
		if subscription.count == subscription.capacity {
			return false
		}
	}
	return true
}

func (feed *SpaceChangefeed) appendHistoryLocked(event SpaceChangefeedEvent) {
	index := (feed.start + feed.count) % feed.capacity
	if feed.count == feed.capacity {
		feed.start = (feed.start + 1) % feed.capacity
		index = (feed.start + feed.count - 1) % feed.capacity
	} else {
		feed.count++
	}
	feed.history[index] = event
}

func (feed *SpaceChangefeed) oldestSequenceLocked() uint64 {
	return feed.next - uint64(feed.count) + 1
}

func (feed *SpaceChangefeed) historyAtLocked(sequence uint64) SpaceChangefeedEvent {
	oldest := feed.oldestSequenceLocked()
	return feed.history[(feed.start+int(sequence-oldest))%feed.capacity]
}

func (feed *SpaceChangefeed) signalLocked() {
	close(feed.changed)
	feed.changed = make(chan struct{})
}

func (subscription *SpaceChangefeedSubscription) enqueueLocked(event SpaceChangefeedEvent) {
	index := (subscription.start + subscription.count) % subscription.capacity
	subscription.buffer[index] = cloneSpaceChangefeedEvent(event)
	subscription.count++
}

func (subscription *SpaceChangefeedSubscription) dequeueLocked() SpaceChangefeedEvent {
	event := subscription.buffer[subscription.start]
	subscription.buffer[subscription.start] = SpaceChangefeedEvent{}
	subscription.start = (subscription.start + 1) % subscription.capacity
	subscription.count--
	return event
}

func (subscription *SpaceChangefeedSubscription) signalLocked() {
	close(subscription.changed)
	subscription.changed = make(chan struct{})
}

func cloneSpaceChangefeedEvent(event SpaceChangefeedEvent) SpaceChangefeedEvent {
	event.Key = append([]byte(nil), event.Key...)
	event.Value = append([]byte(nil), event.Value...)
	return event
}
