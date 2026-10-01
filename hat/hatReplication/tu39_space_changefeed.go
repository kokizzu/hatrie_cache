package hatReplication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultSpaceChangefeedMaxEvents bounds retained changes when the caller
	// does not provide an explicit limit.
	DefaultSpaceChangefeedMaxEvents = 1024
	// DefaultSpaceChangefeedMaxBytes bounds retained payload bytes by default.
	DefaultSpaceChangefeedMaxBytes       = 8 << 20
	MaxSpaceChangefeedSpaceBytes         = 256
	spaceChangefeedCheckpointHeaderBytes = 4 + 1 + 2 + 8 + 8
	MaxSpaceChangefeedCheckpointBytes    = spaceChangefeedCheckpointHeaderBytes + MaxSpaceChangefeedSpaceBytes
)

var (
	ErrSpaceChangefeedInvalid            = errors.New("hatriecache: space changefeed is invalid")
	ErrSpaceChangefeedClosed             = errors.New("hatriecache: space changefeed is closed")
	ErrSpaceChangefeedBackpressure       = errors.New("hatriecache: space changefeed backpressure")
	ErrSpaceChangefeedGap                = errors.New("hatriecache: space changefeed history gap")
	ErrSpaceChangefeedCheckpointInvalid  = errors.New("hatriecache: space changefeed checkpoint is invalid")
	ErrSpaceChangefeedCheckpointMismatch = errors.New("hatriecache: space changefeed checkpoint mismatch")
	ErrSpaceChangefeedAckInvalid         = errors.New("hatriecache: space changefeed acknowledgement is invalid")
	ErrSpaceChangefeedReadLimitInvalid   = errors.New("hatriecache: space changefeed read limit is invalid")
)

var spaceChangefeedCheckpointMagic = [4]byte{'s', 'c', 'p', '1'}

// SpaceChangefeedOperation describes the row-level mutation represented by an
// event. The feed does not interpret row bytes; the owning space defines them.
type SpaceChangefeedOperation uint8

const (
	SpaceChangefeedInsert SpaceChangefeedOperation = iota + 1
	SpaceChangefeedUpdate
	SpaceChangefeedDelete
	SpaceChangefeedReplace
)

// SpaceChangefeedOptions configures one named, schema-bound feed.
type SpaceChangefeedOptions struct {
	Space         string
	SchemaVersion uint64
	MaxEvents     int
	MaxBytes      int
}

// SpaceChangefeedInput is the caller-owned mutation payload passed to Publish.
// Key, Before, and After are copied before Publish returns.
type SpaceChangefeedInput struct {
	Operation SpaceChangefeedOperation
	Key       []byte
	Before    []byte
	After     []byte
}

// SpaceChangefeedEvent is an owned, immutable change returned by Read.
type SpaceChangefeedEvent struct {
	Space         string
	SchemaVersion uint64
	Sequence      uint64
	Operation     SpaceChangefeedOperation
	Key           []byte
	Before        []byte
	After         []byte
}

// SpaceChangefeedCheckpoint identifies the last durably acknowledged event.
// It is bound to both the named space and its schema version.
type SpaceChangefeedCheckpoint struct {
	Space         string
	SchemaVersion uint64
	Sequence      uint64
}

// SpaceChangefeedSubscribeOptions chooses the initial replay position. A zero
// StartSequence reads from the oldest retained event. StartAtLatest starts at
// the current tail. Checkpoint takes precedence over neither option and cannot
// be combined with either one.
type SpaceChangefeedSubscribeOptions struct {
	StartSequence uint64
	StartAtLatest bool
	Checkpoint    *SpaceChangefeedCheckpoint
}

// SpaceChangefeedStats reports bounded in-memory retention state.
type SpaceChangefeedStats struct {
	LastSequence   uint64
	RetainedEvents int
	RetainedBytes  int
	Subscribers    int
	MaxEvents      int
	MaxBytes       int
}

type spaceChangefeedStoredEvent struct {
	Space         string
	SchemaVersion uint64
	Sequence      uint64
	Operation     SpaceChangefeedOperation
	Key           []byte
	Before        []byte
	After         []byte
	bytes         int
}

// SpaceChangefeed is a bounded, in-memory named-space change log. It retains
// events only while every active subscriber has acknowledged past them; when
// that is impossible, Publish returns ErrSpaceChangefeedBackpressure.
type SpaceChangefeed struct {
	mu            sync.Mutex
	space         string
	schemaVersion uint64
	maxEvents     int
	maxBytes      int
	lastSequence  uint64
	retainedBytes int
	events        []spaceChangefeedStoredEvent
	subscribers   map[*SpaceChangefeedSubscription]struct{}
	notify        chan struct{}
	closed        bool
}

// SpaceChangefeedSubscription is a single at-least-once replay cursor.
// Read advances delivery; Ack advances retention and the durable checkpoint.
type SpaceChangefeedSubscription struct {
	feed         *SpaceChangefeed
	delivered    uint64
	acknowledged uint64
	closed       bool
}

// SpaceChangefeedGapError identifies a cursor older than the retained log.
type SpaceChangefeedGapError struct {
	Requested uint64
	Oldest    uint64
}

func (err *SpaceChangefeedGapError) Error() string {
	if err == nil {
		return ErrSpaceChangefeedGap.Error()
	}
	return fmt.Sprintf("%v: requested after %d, oldest %d", ErrSpaceChangefeedGap, err.Requested, err.Oldest)
}

func (err *SpaceChangefeedGapError) Unwrap() error { return ErrSpaceChangefeedGap }

// NewSpaceChangefeed creates a closed-world, bounded changefeed. Zero limits
// use conservative defaults; negative limits are rejected.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space, err := validateSpaceChangefeedIdentity(options.Space, options.SchemaVersion)
	if err != nil {
		return nil, err
	}
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultSpaceChangefeedMaxEvents
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceChangefeedMaxBytes
	}
	if maxEvents < 0 || maxBytes < 0 {
		return nil, ErrSpaceChangefeedInvalid
	}
	return &SpaceChangefeed{
		space:         space,
		schemaVersion: options.SchemaVersion,
		maxEvents:     maxEvents,
		maxBytes:      maxBytes,
		subscribers:   make(map[*SpaceChangefeedSubscription]struct{}),
	}, nil
}

// Publish appends one mutation and returns its assigned monotone sequence.
func (feed *SpaceChangefeed) Publish(input SpaceChangefeedInput) (uint64, error) {
	if feed == nil {
		return 0, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return 0, ErrSpaceChangefeedClosed
	}
	if feed.lastSequence == ^uint64(0) {
		return 0, ErrSpaceChangefeedInvalid
	}
	prepared, err := feed.prepareInputLocked(input)
	if err != nil {
		return 0, err
	}
	if err := feed.makeRoomLocked(1, prepared.bytes); err != nil {
		return 0, err
	}
	prepared.Sequence = feed.lastSequence + 1
	feed.events = append(feed.events, prepared)
	feed.retainedBytes += prepared.bytes
	feed.lastSequence = prepared.Sequence
	feed.signalLocked()
	return prepared.Sequence, nil
}

// PublishBatch appends a batch atomically. No sequence is consumed and no
// event is visible when validation or bounded retention fails.
func (feed *SpaceChangefeed) PublishBatch(inputs []SpaceChangefeedInput) ([]SpaceChangefeedEvent, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedInvalid
	}
	if len(inputs) == 0 {
		return nil, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	if uint64(len(inputs)) > ^uint64(0)-feed.lastSequence {
		return nil, ErrSpaceChangefeedInvalid
	}
	prepared := make([]spaceChangefeedStoredEvent, len(inputs))
	totalBytes := 0
	for index, input := range inputs {
		stored, err := feed.prepareInputLocked(input)
		if err != nil {
			return nil, err
		}
		if totalBytes > int(^uint(0)>>1)-stored.bytes {
			return nil, ErrSpaceChangefeedBackpressure
		}
		totalBytes += stored.bytes
		stored.Sequence = feed.lastSequence + uint64(index) + 1
		prepared[index] = stored
	}
	if err := feed.makeRoomLocked(len(prepared), totalBytes); err != nil {
		return nil, err
	}
	feed.events = append(feed.events, prepared...)
	feed.retainedBytes += totalBytes
	feed.lastSequence += uint64(len(prepared))
	feed.signalLocked()
	result := make([]SpaceChangefeedEvent, len(prepared))
	for index := range prepared {
		result[index] = cloneSpaceChangefeedEvent(prepared[index])
	}
	return result, nil
}

// Subscribe opens a cursor. It never starts a background goroutine.
func (feed *SpaceChangefeed) Subscribe(options SpaceChangefeedSubscribeOptions) (*SpaceChangefeedSubscription, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedInvalid
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	if options.Checkpoint != nil && (options.StartSequence != 0 || options.StartAtLatest) {
		return nil, ErrSpaceChangefeedCheckpointInvalid
	}
	start := options.StartSequence
	if options.Checkpoint != nil {
		checkpoint, err := NewSpaceChangefeedCheckpoint(options.Checkpoint.Space, options.Checkpoint.SchemaVersion, options.Checkpoint.Sequence)
		if err != nil {
			return nil, err
		}
		if checkpoint.Space != feed.space || checkpoint.SchemaVersion != feed.schemaVersion {
			return nil, ErrSpaceChangefeedCheckpointMismatch
		}
		start = checkpoint.Sequence
	}
	if options.StartAtLatest {
		start = feed.lastSequence
	}
	if start > feed.lastSequence {
		return nil, ErrSpaceChangefeedInvalid
	}
	subscription := &SpaceChangefeedSubscription{feed: feed, delivered: start, acknowledged: start}
	feed.subscribers[subscription] = struct{}{}
	feed.evictLocked()
	return subscription, nil
}

// LastSequence returns the current tail sequence.
func (feed *SpaceChangefeed) LastSequence() uint64 {
	if feed == nil {
		return 0
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.lastSequence
}

// Stats returns the current bounded-retention counters.
func (feed *SpaceChangefeed) Stats() SpaceChangefeedStats {
	if feed == nil {
		return SpaceChangefeedStats{}
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return SpaceChangefeedStats{
		LastSequence:   feed.lastSequence,
		RetainedEvents: len(feed.events),
		RetainedBytes:  feed.retainedBytes,
		Subscribers:    len(feed.subscribers),
		MaxEvents:      feed.maxEvents,
		MaxBytes:       feed.maxBytes,
	}
}

// Close stops new publications and wakes current subscribers.
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
	feed.signalLocked()
}

// Read returns up to limit newly delivered events. Events remain retained
// until Ack advances the subscription checkpoint.
func (subscription *SpaceChangefeedSubscription) Read(ctx context.Context, limit int) ([]SpaceChangefeedEvent, error) {
	if subscription == nil || subscription.feed == nil {
		return nil, ErrSpaceChangefeedInvalid
	}
	if limit <= 0 {
		return nil, ErrSpaceChangefeedReadLimitInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		feed := subscription.feed
		feed.mu.Lock()
		if subscription.closed || feed.closed {
			feed.mu.Unlock()
			return nil, ErrSpaceChangefeedClosed
		}
		if err := feed.gapErrorLocked(subscription.delivered); err != nil {
			feed.mu.Unlock()
			return nil, err
		}
		if index := feed.firstEventAfterLocked(subscription.delivered); index >= 0 {
			end := index + limit
			if end > len(feed.events) {
				end = len(feed.events)
			}
			result := make([]SpaceChangefeedEvent, end-index)
			for resultIndex := range result {
				result[resultIndex] = cloneSpaceChangefeedEvent(feed.events[index+resultIndex])
			}
			subscription.delivered = result[len(result)-1].Sequence
			feed.mu.Unlock()
			return result, nil
		}
		notify := feed.waitChannelLocked()
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

// Wait blocks until Read can deliver at least one event, the feed closes, or
// ctx is cancelled.
func (subscription *SpaceChangefeedSubscription) Wait(ctx context.Context) error {
	if subscription == nil || subscription.feed == nil {
		return ErrSpaceChangefeedInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		feed := subscription.feed
		feed.mu.Lock()
		if subscription.closed || feed.closed {
			feed.mu.Unlock()
			return ErrSpaceChangefeedClosed
		}
		if err := feed.gapErrorLocked(subscription.delivered); err != nil {
			feed.mu.Unlock()
			return err
		}
		if feed.firstEventAfterLocked(subscription.delivered) >= 0 {
			feed.mu.Unlock()
			return nil
		}
		notify := feed.waitChannelLocked()
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// Ack advances the at-least-once checkpoint. It cannot acknowledge data that
// has not been delivered by this subscription.
func (subscription *SpaceChangefeedSubscription) Ack(sequence uint64) error {
	if subscription == nil || subscription.feed == nil {
		return ErrSpaceChangefeedInvalid
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.closed || feed.closed {
		return ErrSpaceChangefeedClosed
	}
	if sequence < subscription.acknowledged || sequence > subscription.delivered {
		return ErrSpaceChangefeedAckInvalid
	}
	subscription.acknowledged = sequence
	feed.evictLocked()
	return nil
}

// Checkpoint returns the last acknowledged position, not merely the last
// delivered position, so a restarted consumer replays unacknowledged events.
func (subscription *SpaceChangefeedSubscription) Checkpoint() (SpaceChangefeedCheckpoint, error) {
	if subscription == nil || subscription.feed == nil {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedInvalid
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.closed || feed.closed {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedClosed
	}
	return SpaceChangefeedCheckpoint{Space: feed.space, SchemaVersion: feed.schemaVersion, Sequence: subscription.acknowledged}, nil
}

// Close removes the subscription and releases its retention obligation.
func (subscription *SpaceChangefeedSubscription) Close() {
	if subscription == nil || subscription.feed == nil {
		return
	}
	feed := subscription.feed
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if subscription.closed {
		return
	}
	subscription.closed = true
	delete(feed.subscribers, subscription)
	feed.evictLocked()
	feed.signalLocked()
}

// NewSpaceChangefeedCheckpoint creates a validated checkpoint.
func NewSpaceChangefeedCheckpoint(space string, schemaVersion, sequence uint64) (SpaceChangefeedCheckpoint, error) {
	validatedSpace, err := validateSpaceChangefeedIdentity(space, schemaVersion)
	if err != nil {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	return SpaceChangefeedCheckpoint{Space: validatedSpace, SchemaVersion: schemaVersion, Sequence: sequence}, nil
}

// MarshalBinary encodes a bounded, deterministic checkpoint.
func (checkpoint SpaceChangefeedCheckpoint) MarshalBinary() ([]byte, error) {
	validated, err := NewSpaceChangefeedCheckpoint(checkpoint.Space, checkpoint.SchemaVersion, checkpoint.Sequence)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, spaceChangefeedCheckpointHeaderBytes+len(validated.Space))
	copy(encoded[:len(spaceChangefeedCheckpointMagic)], spaceChangefeedCheckpointMagic[:])
	encoded[4] = 1
	binary.BigEndian.PutUint16(encoded[5:7], uint16(len(validated.Space)))
	binary.BigEndian.PutUint64(encoded[7:15], validated.SchemaVersion)
	binary.BigEndian.PutUint64(encoded[15:23], validated.Sequence)
	copy(encoded[spaceChangefeedCheckpointHeaderBytes:], validated.Space)
	return encoded, nil
}

// UnmarshalSpaceChangefeedCheckpoint validates and owns a checkpoint payload.
func UnmarshalSpaceChangefeedCheckpoint(encoded []byte) (SpaceChangefeedCheckpoint, error) {
	if len(encoded) < spaceChangefeedCheckpointHeaderBytes || len(encoded) > MaxSpaceChangefeedCheckpointBytes {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	if !bytes.Equal(encoded[:len(spaceChangefeedCheckpointMagic)], spaceChangefeedCheckpointMagic[:]) || encoded[4] != 1 {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	spaceLength := int(binary.BigEndian.Uint16(encoded[5:7]))
	if spaceLength == 0 || spaceLength > MaxSpaceChangefeedSpaceBytes || len(encoded) != spaceChangefeedCheckpointHeaderBytes+spaceLength {
		return SpaceChangefeedCheckpoint{}, ErrSpaceChangefeedCheckpointInvalid
	}
	return NewSpaceChangefeedCheckpoint(
		string(encoded[spaceChangefeedCheckpointHeaderBytes:]),
		binary.BigEndian.Uint64(encoded[7:15]),
		binary.BigEndian.Uint64(encoded[15:23]),
	)
}

func validateSpaceChangefeedIdentity(space string, schemaVersion uint64) (string, error) {
	if strings.TrimSpace(space) != space || space == "" || len(space) > MaxSpaceChangefeedSpaceBytes || schemaVersion == 0 {
		return "", ErrSpaceChangefeedInvalid
	}
	return space, nil
}

func (feed *SpaceChangefeed) prepareInputLocked(input SpaceChangefeedInput) (spaceChangefeedStoredEvent, error) {
	if input.Operation < SpaceChangefeedInsert || input.Operation > SpaceChangefeedReplace || len(input.Key) == 0 {
		return spaceChangefeedStoredEvent{}, ErrSpaceChangefeedInvalid
	}
	bytesCount := spaceChangefeedEventFixedBytes + len(input.Key) + len(input.Before) + len(input.After)
	if bytesCount < 0 {
		return spaceChangefeedStoredEvent{}, spaceChangefeedBackpressureError()
	}
	return spaceChangefeedStoredEvent{
		Space:         feed.space,
		SchemaVersion: feed.schemaVersion,
		Operation:     input.Operation,
		Key:           append([]byte(nil), input.Key...),
		Before:        append([]byte(nil), input.Before...),
		After:         append([]byte(nil), input.After...),
		bytes:         bytesCount,
	}, nil
}

const spaceChangefeedEventFixedBytes = 32

func spaceChangefeedBackpressureError() error { return ErrSpaceChangefeedBackpressure }

func (feed *SpaceChangefeed) makeRoomLocked(addEvents, addBytes int) error {
	if addEvents > feed.maxEvents || addBytes > feed.maxBytes {
		return ErrSpaceChangefeedBackpressure
	}
	for len(feed.events)+addEvents > feed.maxEvents || feed.retainedBytes+addBytes > feed.maxBytes {
		if len(feed.events) == 0 || !feed.canEvictLocked(feed.events[0].Sequence) {
			return ErrSpaceChangefeedBackpressure
		}
		feed.retainedBytes -= feed.events[0].bytes
		copy(feed.events, feed.events[1:])
		feed.events = feed.events[:len(feed.events)-1]
	}
	return nil
}

func (feed *SpaceChangefeed) canEvictLocked(sequence uint64) bool {
	for subscription := range feed.subscribers {
		if !subscription.closed && subscription.acknowledged < sequence {
			return false
		}
	}
	return true
}

func (feed *SpaceChangefeed) evictLocked() {
	for len(feed.events) > 0 && feed.canEvictLocked(feed.events[0].Sequence) {
		feed.retainedBytes -= feed.events[0].bytes
		copy(feed.events, feed.events[1:])
		feed.events = feed.events[:len(feed.events)-1]
	}
}

func (feed *SpaceChangefeed) firstEventAfterLocked(sequence uint64) int {
	for index := range feed.events {
		if feed.events[index].Sequence > sequence {
			return index
		}
	}
	return -1
}

func (feed *SpaceChangefeed) gapErrorLocked(sequence uint64) error {
	if len(feed.events) == 0 || sequence >= feed.events[0].Sequence-1 {
		return nil
	}
	return &SpaceChangefeedGapError{Requested: sequence, Oldest: feed.events[0].Sequence}
}

func (feed *SpaceChangefeed) signalLocked() {
	if feed.notify == nil {
		return
	}
	close(feed.notify)
	feed.notify = nil
}

func (feed *SpaceChangefeed) waitChannelLocked() chan struct{} {
	if feed.notify == nil {
		feed.notify = make(chan struct{})
	}
	return feed.notify
}

func cloneSpaceChangefeedEvent(event spaceChangefeedStoredEvent) SpaceChangefeedEvent {
	return SpaceChangefeedEvent{
		Space:         event.Space,
		SchemaVersion: event.SchemaVersion,
		Sequence:      event.Sequence,
		Operation:     event.Operation,
		Key:           append([]byte(nil), event.Key...),
		Before:        append([]byte(nil), event.Before...),
		After:         append([]byte(nil), event.After...),
	}
}
