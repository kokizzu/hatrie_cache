package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	DefaultSpaceChangefeedMaxEvents      = 4096
	MaxSpaceChangefeedMaxEvents          = 1 << 20
	DefaultSpaceChangefeedMaxSubscribers = 256
	MaxSpaceChangefeedMaxSubscribers     = 65536
	DefaultSpaceChangefeedBuffer         = 256
	MaxSpaceChangefeedBuffer             = 65536
	DefaultSpaceChangefeedMaxEventBytes  = 1 << 20
	MaxSpaceChangefeedMaxEventBytes      = 16 << 20
	DefaultSpaceChangefeedMaxBytes       = 64 << 20
	MaxSpaceChangefeedMaxBytes           = 1 << 30
	MaxSpaceChangefeedNameBytes          = 256
)

var (
	ErrSpaceChangefeedNil              = errors.New("hatriecache: space changefeed is nil")
	ErrSpaceChangefeedClosed           = errors.New("hatriecache: space changefeed is closed")
	ErrSpaceChangefeedSpaceRequired    = errors.New("hatriecache: space changefeed space is required")
	ErrSpaceChangefeedSchemaRequired   = errors.New("hatriecache: space changefeed schema version is required")
	ErrSpaceChangefeedOptionsInvalid   = errors.New("hatriecache: space changefeed options are invalid")
	ErrSpaceChangefeedEventInvalid     = errors.New("hatriecache: space changefeed event is invalid")
	ErrSpaceChangefeedEventTooLarge    = errors.New("hatriecache: space changefeed event is too large")
	ErrSpaceChangefeedSchemaMismatch   = errors.New("hatriecache: space changefeed schema version mismatch")
	ErrSpaceChangefeedCheckpointSource = errors.New("hatriecache: space changefeed checkpoint source mismatch")
	ErrSpaceChangefeedCheckpointAhead  = errors.New("hatriecache: space changefeed checkpoint is ahead of delivery")
	ErrSpaceChangefeedHistoryGap       = errors.New("hatriecache: space changefeed history gap")
	ErrSpaceChangefeedReplayLimit      = errors.New("hatriecache: space changefeed replay exceeds subscription buffer")
	ErrSpaceChangefeedOverflow         = errors.New("hatriecache: space changefeed subscriber overflowed")
	ErrSpaceChangefeedSubscriberLimit  = errors.New("hatriecache: space changefeed subscriber limit reached")
)

// SpaceChangefeedOperation identifies the row mutation represented by an event.
type SpaceChangefeedOperation uint8

const (
	SpaceChangefeedInsert SpaceChangefeedOperation = iota + 1
	SpaceChangefeedUpdate
	SpaceChangefeedUpsert
	SpaceChangefeedDelete
)

// SpaceChangefeedEvent is an immutable-on-publication row change. Key, Before,
// and After are copied by Publish and by each subscription delivery.
type SpaceChangefeedEvent struct {
	Sequence      uint64
	Space         string
	SchemaVersion string
	Operation     SpaceChangefeedOperation
	Key           []byte
	Before        []byte
	After         []byte
}

// SpaceChangefeedOptions bounds retained history and identifies one logical
// space. Zero limits select conservative defaults; the feed never starts a
// worker and never performs network or filesystem work.
type SpaceChangefeedOptions struct {
	Space          string
	SchemaVersion  string
	MaxEvents      int
	MaxSubscribers int
	MaxBytes       int64
	MaxEventBytes  int
}

// SpaceChangefeedSubscribeOptions controls replay and consumer backpressure.
// Checkpoint is the last event the consumer durably applied. A zero checkpoint
// starts at the oldest retained event. ExpectedSchemaVersion may be left empty
// to accept the feed's immutable schema version.
type SpaceChangefeedSubscribeOptions struct {
	Checkpoint            ChangefeedCheckpoint
	ExpectedSchemaVersion string
	Buffer                int
}

// SpaceChangefeedStats is a point-in-time bounded history and subscriber view.
type SpaceChangefeedStats struct {
	Space                 string
	SchemaVersion         string
	NextSequence          uint64
	Published             uint64
	RetainedEvents        int
	RetainedBytes         int64
	Subscribers           int
	OverflowedSubscribers uint64
}

type spaceChangefeedSubscription struct {
	feed       *SpaceChangefeed
	events     chan SpaceChangefeedEvent
	done       chan struct{}
	checkpoint uint64
	lastSent   uint64
	err        error
	closed     bool
}

// SpaceChangefeed is a bounded, named-space changefeed with explicit schema
// identity and consumer-owned checkpoints. Publish never waits for a consumer:
// a full subscriber buffer is terminated with ErrSpaceChangefeedOverflow.
type SpaceChangefeed struct {
	mu sync.Mutex

	space          string
	schemaVersion  string
	maxEvents      int
	maxSubscribers int
	maxBytes       int64
	maxEventBytes  int
	events         []SpaceChangefeedEvent
	eventHead      int
	eventCount     int
	retainedBytes  int64
	nextSequence   uint64
	published      uint64
	overflowed     uint64
	subscribers    map[*spaceChangefeedSubscription]struct{}
	closed         bool
}

// NewSpaceChangefeed creates a bounded feed for one named logical space.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space, err := normalizeSpaceChangefeedName(options.Space, ErrSpaceChangefeedSpaceRequired)
	if err != nil {
		return nil, err
	}
	schemaVersion, err := normalizeSpaceChangefeedName(options.SchemaVersion, ErrSpaceChangefeedSchemaRequired)
	if err != nil {
		return nil, err
	}
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultSpaceChangefeedMaxEvents
	}
	maxSubscribers := options.MaxSubscribers
	if maxSubscribers == 0 {
		maxSubscribers = DefaultSpaceChangefeedMaxSubscribers
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceChangefeedMaxBytes
	}
	maxEventBytes := options.MaxEventBytes
	if maxEventBytes == 0 {
		maxEventBytes = DefaultSpaceChangefeedMaxEventBytes
	}
	if maxEvents < 1 || maxEvents > MaxSpaceChangefeedMaxEvents || maxSubscribers < 1 || maxSubscribers > MaxSpaceChangefeedMaxSubscribers || maxBytes < 1 || maxBytes > MaxSpaceChangefeedMaxBytes || maxEventBytes < 1 || maxEventBytes > MaxSpaceChangefeedMaxEventBytes || int64(maxEventBytes) > maxBytes {
		return nil, ErrSpaceChangefeedOptionsInvalid
	}
	return &SpaceChangefeed{
		space:          space,
		schemaVersion:  schemaVersion,
		maxEvents:      maxEvents,
		maxSubscribers: maxSubscribers,
		maxBytes:       maxBytes,
		maxEventBytes:  maxEventBytes,
		subscribers:    make(map[*spaceChangefeedSubscription]struct{}),
	}, nil
}

// Space returns the feed's normalized logical-space name.
func (feed *SpaceChangefeed) Space() string {
	if feed == nil {
		return ""
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.space
}

// SchemaVersion returns the immutable schema identity carried by every event.
func (feed *SpaceChangefeed) SchemaVersion() string {
	if feed == nil {
		return ""
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.schemaVersion
}

// Publish appends one event and returns its monotone feed sequence. The caller
// may omit Space and SchemaVersion; the feed supplies its own values.
func (feed *SpaceChangefeed) Publish(event SpaceChangefeedEvent) (uint64, error) {
	if feed == nil {
		return 0, ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return 0, ErrSpaceChangefeedClosed
	}
	normalized, eventBytes, err := feed.normalizeEventLocked(event)
	if err != nil {
		return 0, err
	}
	if feed.nextSequence == ^uint64(0) {
		return 0, fmt.Errorf("%w: sequence exhausted", ErrSpaceChangefeedEventInvalid)
	}
	normalized.Sequence = feed.nextSequence + 1
	feed.nextSequence = normalized.Sequence
	feed.published++
	feed.appendEventLocked(normalized, eventBytes)
	for subscription := range feed.subscribers {
		if subscription.closed {
			continue
		}
		select {
		case subscription.events <- cloneSpaceChangefeedEvent(normalized):
			subscription.lastSent = normalized.Sequence
		default:
			feed.closeSubscriptionLocked(subscription, ErrSpaceChangefeedOverflow)
			feed.overflowed++
		}
	}
	return normalized.Sequence, nil
}

// Subscribe creates a bounded consumer and replays retained events after its
// checkpoint. A checkpoint older than retained history fails closed instead of
// silently dropping data.
func (feed *SpaceChangefeed) Subscribe(ctx context.Context, options SpaceChangefeedSubscribeOptions) (*SpaceChangefeedSubscription, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedNil
	}
	if ctx == nil {
		return nil, ErrSpaceChangefeedOptionsInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	if len(feed.subscribers) >= feed.maxSubscribers {
		return nil, ErrSpaceChangefeedSubscriberLimit
	}
	expected := strings.TrimSpace(options.ExpectedSchemaVersion)
	if expected != "" && expected != feed.schemaVersion {
		return nil, fmt.Errorf("%w: expected=%q actual=%q", ErrSpaceChangefeedSchemaMismatch, expected, feed.schemaVersion)
	}
	checkpoint := options.Checkpoint
	if checkpoint.Source != "" && checkpoint.Source != feed.space {
		return nil, ErrSpaceChangefeedCheckpointSource
	}
	if checkpoint.Sequence > feed.nextSequence {
		return nil, ErrSpaceChangefeedCheckpointAhead
	}
	if feed.eventCount > 0 {
		oldest := feed.eventAtLocked(0).Sequence
		if checkpoint.Sequence < oldest-1 {
			return nil, fmt.Errorf("%w: checkpoint=%d oldest=%d", ErrSpaceChangefeedHistoryGap, checkpoint.Sequence, oldest)
		}
	}
	buffer := options.Buffer
	if buffer == 0 {
		buffer = DefaultSpaceChangefeedBuffer
	}
	if buffer < 1 || buffer > MaxSpaceChangefeedBuffer {
		return nil, ErrSpaceChangefeedOptionsInvalid
	}
	replayCount := 0
	for index := 0; index < feed.eventCount; index++ {
		if feed.eventAtLocked(index).Sequence > checkpoint.Sequence {
			replayCount++
		}
	}
	if replayCount > buffer {
		return nil, fmt.Errorf("%w: replay=%d buffer=%d", ErrSpaceChangefeedReplayLimit, replayCount, buffer)
	}
	subscription := &spaceChangefeedSubscription{
		feed:       feed,
		events:     make(chan SpaceChangefeedEvent, buffer),
		done:       make(chan struct{}),
		checkpoint: checkpoint.Sequence,
		lastSent:   checkpoint.Sequence,
	}
	for index := 0; index < feed.eventCount; index++ {
		event := feed.eventAtLocked(index)
		if event.Sequence > checkpoint.Sequence {
			subscription.events <- cloneSpaceChangefeedEvent(event)
			subscription.lastSent = event.Sequence
		}
	}
	feed.subscribers[subscription] = struct{}{}
	if done := ctx.Done(); done != nil {
		go feed.watchSpaceChangefeedContext(ctx, subscription)
	}
	return &SpaceChangefeedSubscription{state: subscription}, nil
}

// Stats returns bounded feed and subscriber accounting.
func (feed *SpaceChangefeed) Stats() SpaceChangefeedStats {
	if feed == nil {
		return SpaceChangefeedStats{}
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return SpaceChangefeedStats{
		Space:                 feed.space,
		SchemaVersion:         feed.schemaVersion,
		NextSequence:          feed.nextSequence,
		Published:             feed.published,
		RetainedEvents:        feed.eventCount,
		RetainedBytes:         feed.retainedBytes,
		Subscribers:           len(feed.subscribers),
		OverflowedSubscribers: feed.overflowed,
	}
}

// Close terminates the feed and all active subscriptions.
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
	for subscription := range feed.subscribers {
		feed.closeSubscriptionLocked(subscription, ErrSpaceChangefeedClosed)
	}
}

func (feed *SpaceChangefeed) normalizeEventLocked(event SpaceChangefeedEvent) (SpaceChangefeedEvent, int64, error) {
	if event.Sequence != 0 || !validSpaceChangefeedOperation(event.Operation) || len(event.Key) == 0 {
		return SpaceChangefeedEvent{}, 0, ErrSpaceChangefeedEventInvalid
	}
	space := strings.TrimSpace(event.Space)
	if space == "" {
		space = feed.space
	}
	if space != feed.space {
		return SpaceChangefeedEvent{}, 0, ErrSpaceChangefeedEventInvalid
	}
	schemaVersion := strings.TrimSpace(event.SchemaVersion)
	if schemaVersion == "" {
		schemaVersion = feed.schemaVersion
	}
	if schemaVersion != feed.schemaVersion {
		return SpaceChangefeedEvent{}, 0, ErrSpaceChangefeedSchemaMismatch
	}
	bytes, ok := spaceChangefeedEventBytes(event, feed.maxEventBytes)
	if !ok {
		return SpaceChangefeedEvent{}, 0, ErrSpaceChangefeedEventTooLarge
	}
	normalized := SpaceChangefeedEvent{
		Space:         feed.space,
		SchemaVersion: feed.schemaVersion,
		Operation:     event.Operation,
		Key:           cloneBytes(event.Key),
		Before:        cloneBytes(event.Before),
		After:         cloneBytes(event.After),
	}
	return normalized, bytes, nil
}

func (feed *SpaceChangefeed) appendEventLocked(event SpaceChangefeedEvent, eventBytes int64) {
	if feed.eventCount == len(feed.events) && len(feed.events) == feed.maxEvents {
		oldest := feed.eventAtLocked(0)
		feed.retainedBytes -= spaceChangefeedEventBytesUnchecked(oldest)
		feed.events[feed.eventHead] = event
		feed.eventHead = (feed.eventHead + 1) % len(feed.events)
		feed.retainedBytes += eventBytes
		return
	}
	feed.ensureEventCapacityLocked()
	index := (feed.eventHead + feed.eventCount) % len(feed.events)
	feed.events[index] = event
	feed.eventCount++
	feed.retainedBytes += eventBytes
	for feed.eventCount > feed.maxEvents || feed.retainedBytes > feed.maxBytes {
		oldest := feed.eventAtLocked(0)
		feed.retainedBytes -= spaceChangefeedEventBytesUnchecked(oldest)
		feed.events[feed.eventHead] = SpaceChangefeedEvent{}
		feed.eventHead = (feed.eventHead + 1) % len(feed.events)
		feed.eventCount--
	}
}

func (feed *SpaceChangefeed) ensureEventCapacityLocked() {
	if feed.eventCount < len(feed.events) {
		return
	}
	capacity := len(feed.events) * 2
	if capacity == 0 {
		capacity = 16
	}
	if capacity > feed.maxEvents {
		capacity = feed.maxEvents
	}
	resized := make([]SpaceChangefeedEvent, capacity)
	for index := 0; index < feed.eventCount; index++ {
		resized[index] = feed.eventAtLocked(index)
	}
	feed.events = resized
	feed.eventHead = 0
}

func (feed *SpaceChangefeed) eventAtLocked(offset int) SpaceChangefeedEvent {
	return feed.events[(feed.eventHead+offset)%len(feed.events)]
}

func (feed *SpaceChangefeed) closeSubscriptionLocked(subscription *spaceChangefeedSubscription, err error) {
	if subscription.closed {
		return
	}
	subscription.closed = true
	subscription.err = err
	delete(feed.subscribers, subscription)
	close(subscription.events)
	close(subscription.done)
}

func (feed *SpaceChangefeed) watchSpaceChangefeedContext(ctx context.Context, subscription *spaceChangefeedSubscription) {
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err == nil {
			err = ErrSpaceChangefeedClosed
		}
		feed.mu.Lock()
		feed.closeSubscriptionLocked(subscription, err)
		feed.mu.Unlock()
	case <-subscription.done:
	}
}

func normalizeSpaceChangefeedName(value string, required error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", required
	}
	if len(value) > MaxSpaceChangefeedNameBytes || strings.IndexByte(value, 0) >= 0 {
		return "", ErrSpaceChangefeedOptionsInvalid
	}
	return value, nil
}

func validSpaceChangefeedOperation(operation SpaceChangefeedOperation) bool {
	return operation >= SpaceChangefeedInsert && operation <= SpaceChangefeedDelete
}

func spaceChangefeedEventBytes(event SpaceChangefeedEvent, max int) (int64, bool) {
	total := 0
	for _, value := range [][]byte{event.Key, event.Before, event.After} {
		if len(value) > max || total > max-len(value) {
			return 0, false
		}
		total += len(value)
	}
	return int64(total), true
}

func spaceChangefeedEventBytesUnchecked(event SpaceChangefeedEvent) int64 {
	return int64(len(event.Key) + len(event.Before) + len(event.After))
}

func cloneSpaceChangefeedEvent(event SpaceChangefeedEvent) SpaceChangefeedEvent {
	event.Key = cloneBytes(event.Key)
	event.Before = cloneBytes(event.Before)
	event.After = cloneBytes(event.After)
	return event
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return append([]byte(nil), value...)
}

// SpaceChangefeedSubscription is a bounded consumer view. The consumer must
// call Advance only after it has durably applied the delivered event.
type SpaceChangefeedSubscription struct {
	state *spaceChangefeedSubscription
}

// Events returns the ordered event channel. It is closed on Close, overflow,
// feed shutdown, or context cancellation.
func (subscription *SpaceChangefeedSubscription) Events() <-chan SpaceChangefeedEvent {
	if subscription == nil || subscription.state == nil {
		return nil
	}
	return subscription.state.events
}

// Err returns the terminal error, if any. Explicit Close returns nil.
func (subscription *SpaceChangefeedSubscription) Err() error {
	if subscription == nil || subscription.state == nil || subscription.state.feed == nil {
		return nil
	}
	feed := subscription.state.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return subscription.state.err
}

// Advance records the greatest event sequence durably applied by the consumer.
func (subscription *SpaceChangefeedSubscription) Advance(sequence uint64) (ChangefeedCheckpoint, error) {
	if subscription == nil || subscription.state == nil || subscription.state.feed == nil {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedNil
	}
	feed := subscription.state.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	state := subscription.state
	if state.closed {
		if state.err != nil {
			return ChangefeedCheckpoint{}, state.err
		}
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedClosed
	}
	if sequence > state.lastSent {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointAhead
	}
	checkpoint, err := ChangefeedCheckpoint{Source: feed.space, Sequence: state.checkpoint}.Advance(ChangefeedProgress{Sequence: sequence, Progressed: true})
	if err != nil {
		return ChangefeedCheckpoint{}, err
	}
	state.checkpoint = checkpoint.Sequence
	return checkpoint, nil
}

// Checkpoint returns the latest explicitly acknowledged consumer sequence.
func (subscription *SpaceChangefeedSubscription) Checkpoint() (ChangefeedCheckpoint, error) {
	if subscription == nil || subscription.state == nil || subscription.state.feed == nil {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedNil
	}
	feed := subscription.state.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if err := subscription.stateErrLocked(); err != nil {
		return ChangefeedCheckpoint{}, err
	}
	return ChangefeedCheckpoint{Source: feed.space, Sequence: subscription.state.checkpoint}, nil
}

// Close removes the consumer without changing retained feed history.
func (subscription *SpaceChangefeedSubscription) Close() {
	if subscription == nil || subscription.state == nil || subscription.state.feed == nil {
		return
	}
	feed := subscription.state.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.state.closed {
		return
	}
	feed.closeSubscriptionLocked(subscription.state, nil)
}

func (subscription *SpaceChangefeedSubscription) stateErrLocked() error {
	if subscription.state.closed && subscription.state.err != nil {
		return subscription.state.err
	}
	if subscription.state.closed {
		return ErrSpaceChangefeedClosed
	}
	return nil
}
