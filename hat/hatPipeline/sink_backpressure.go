package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrSinkBackpressureRegistryNil indicates a method was called on a nil
	// registry.
	ErrSinkBackpressureRegistryNil = errors.New("hatPipeline: sink backpressure registry is nil")
	// ErrSinkBackpressureNameRequired indicates that a sink name is empty.
	ErrSinkBackpressureNameRequired = errors.New("hatPipeline: sink backpressure sink name is required")
	// ErrSinkBackpressureAlreadyRegistered indicates a duplicate sink name.
	ErrSinkBackpressureAlreadyRegistered = errors.New("hatPipeline: sink backpressure sink is already registered")
	// ErrSinkBackpressureMissing indicates an unknown sink name.
	ErrSinkBackpressureMissing = errors.New("hatPipeline: sink backpressure sink is missing")
	// ErrSinkBackpressureClosed indicates that the registry no longer accepts
	// updates or waits.
	ErrSinkBackpressureClosed = errors.New("hatPipeline: sink backpressure registry is closed")
	// ErrSinkBackpressureOptionsInvalid indicates invalid watermark bounds.
	ErrSinkBackpressureOptionsInvalid = errors.New("hatPipeline: sink backpressure options are invalid")
	// ErrSinkBackpressureRegression indicates a frontier moved backward.
	ErrSinkBackpressureRegression = errors.New("hatPipeline: sink backpressure frontier regressed")
	// ErrSinkBackpressureAcknowledgementAhead indicates that a sink acknowledged
	// a frontier it has not emitted.
	ErrSinkBackpressureAcknowledgementAhead = errors.New("hatPipeline: sink acknowledgement is ahead of emitted frontier")
)

const (
	// DefaultSinkBackpressureHighWatermark enters blocked state at lag 1024.
	DefaultSinkBackpressureHighWatermark uint64 = 1024
	// DefaultSinkBackpressureLowWatermark leaves blocked state at lag 512.
	DefaultSinkBackpressureLowWatermark uint64 = 512
	// MaxSinkBackpressureWatermark bounds configured thresholds and avoids
	// accidental values that are difficult to operate.
	MaxSinkBackpressureWatermark uint64 = 1 << 62
)

// SinkBackpressureRegistryOptions controls defaults for newly registered
// sinks. Zero values select the exported sane defaults; a zero low watermark
// derives half of the selected high watermark.
type SinkBackpressureRegistryOptions struct {
	DefaultHighWatermark uint64
	DefaultLowWatermark  uint64
}

// SinkBackpressureSinkOptions overrides the registry defaults for one sink.
// A zero HighWatermark uses the registry default. A zero LowWatermark derives
// half of the selected high watermark.
type SinkBackpressureSinkOptions struct {
	HighWatermark uint64
	LowWatermark  uint64
}

// SinkBackpressureStatus is a point-in-time frontier and admission snapshot.
// Lag is EmittedFrontier minus AcknowledgedFrontier. Blocked remains true
// until lag reaches the configured low watermark, which prevents oscillation
// around the high watermark.
type SinkBackpressureStatus struct {
	Sink                 string
	EmittedFrontier      uint64
	AcknowledgedFrontier uint64
	Lag                  uint64
	HighWatermark        uint64
	LowWatermark         uint64
	Blocked              bool
	UpdatedAt            time.Time
}

type sinkBackpressureState struct {
	status SinkBackpressureStatus
	notify chan struct{}
}

// SinkBackpressureRegistry is a bounded, frontier-aware admission catalog for
// independent sinks. It does not own queues or transport connections: a
// producer calls WaitUntilWritable before submitting work, and the sink calls
// Advance/Acknowledge or Record as frontiers move.
type SinkBackpressureRegistry struct {
	mu                   sync.RWMutex
	sinks                map[string]*sinkBackpressureState
	defaultHighWatermark uint64
	defaultLowWatermark  uint64
	closed               bool
}

// NewSinkBackpressureRegistry creates an empty sink backpressure catalog.
func NewSinkBackpressureRegistry(options SinkBackpressureRegistryOptions) (*SinkBackpressureRegistry, error) {
	high := options.DefaultHighWatermark
	if high == 0 {
		high = DefaultSinkBackpressureHighWatermark
	}
	low := options.DefaultLowWatermark
	if low == 0 {
		low = high / 2
	}
	if err := validateSinkBackpressureWatermarks(high, low); err != nil {
		return nil, err
	}
	return &SinkBackpressureRegistry{
		sinks:                make(map[string]*sinkBackpressureState),
		defaultHighWatermark: high,
		defaultLowWatermark:  low,
	}, nil
}

// Register adds a sink with zero frontiers and configured hysteresis.
func (registry *SinkBackpressureRegistry) Register(sink string, options SinkBackpressureSinkOptions) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrSinkBackpressureClosed
	}
	if _, exists := registry.sinks[sink]; exists {
		return ErrSinkBackpressureAlreadyRegistered
	}
	high, low, err := normalizeSinkBackpressureWatermarks(options.HighWatermark, options.LowWatermark, registry.defaultHighWatermark, registry.defaultLowWatermark)
	if err != nil {
		return err
	}
	registry.sinks[sink] = &sinkBackpressureState{
		status: SinkBackpressureStatus{
			Sink:          sink,
			HighWatermark: high,
			LowWatermark:  low,
			UpdatedAt:     time.Now().UTC(),
		},
		notify: make(chan struct{}),
	}
	return nil
}

// Unregister removes a sink and wakes waiters for it.
func (registry *SinkBackpressureRegistry) Unregister(sink string) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrSinkBackpressureClosed
	}
	state, exists := registry.sinks[sink]
	if !exists {
		return ErrSinkBackpressureMissing
	}
	delete(registry.sinks, sink)
	close(state.notify)
	return nil
}

// Advance moves the emitted frontier forward and may enter blocked state.
func (registry *SinkBackpressureRegistry) Advance(sink string, emitted uint64) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrSinkBackpressureClosed
	}
	state, exists := registry.sinks[sink]
	if !exists {
		return ErrSinkBackpressureMissing
	}
	if emitted < state.status.EmittedFrontier || emitted < state.status.AcknowledgedFrontier {
		return ErrSinkBackpressureRegression
	}
	if emitted == state.status.EmittedFrontier {
		return nil
	}
	state.status.EmittedFrontier = emitted
	registry.refreshSinkLocked(state)
	return nil
}

// Acknowledge moves the acknowledged frontier forward and may release
// blocked producers when the low watermark is reached.
func (registry *SinkBackpressureRegistry) Acknowledge(sink string, acknowledged uint64) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrSinkBackpressureClosed
	}
	state, exists := registry.sinks[sink]
	if !exists {
		return ErrSinkBackpressureMissing
	}
	if acknowledged < state.status.AcknowledgedFrontier {
		return ErrSinkBackpressureRegression
	}
	if acknowledged > state.status.EmittedFrontier {
		return ErrSinkBackpressureAcknowledgementAhead
	}
	if acknowledged == state.status.AcknowledgedFrontier {
		return nil
	}
	state.status.AcknowledgedFrontier = acknowledged
	registry.refreshSinkLocked(state)
	return nil
}

// Record atomically advances both frontiers for one sink.
func (registry *SinkBackpressureRegistry) Record(sink string, emitted, acknowledged uint64) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	if acknowledged > emitted {
		return ErrSinkBackpressureAcknowledgementAhead
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrSinkBackpressureClosed
	}
	state, exists := registry.sinks[sink]
	if !exists {
		return ErrSinkBackpressureMissing
	}
	if emitted < state.status.EmittedFrontier || acknowledged < state.status.AcknowledgedFrontier {
		return ErrSinkBackpressureRegression
	}
	if emitted == state.status.EmittedFrontier && acknowledged == state.status.AcknowledgedFrontier {
		return nil
	}
	state.status.EmittedFrontier = emitted
	state.status.AcknowledgedFrontier = acknowledged
	registry.refreshSinkLocked(state)
	return nil
}

// WaitUntilWritable blocks while a sink is above its low-watermark release
// boundary. It wakes on acknowledgement, unregister, close, or cancellation.
func (registry *SinkBackpressureRegistry) WaitUntilWritable(ctx context.Context, sink string) error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	sink, err := normalizeSinkBackpressureName(sink)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		registry.mu.RLock()
		if registry.closed {
			registry.mu.RUnlock()
			return ErrSinkBackpressureClosed
		}
		state, exists := registry.sinks[sink]
		if !exists {
			registry.mu.RUnlock()
			return ErrSinkBackpressureMissing
		}
		if !state.status.Blocked {
			registry.mu.RUnlock()
			return nil
		}
		notify := state.notify
		registry.mu.RUnlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Status returns the current status for one sink.
func (registry *SinkBackpressureRegistry) Status(sink string) (SinkBackpressureStatus, bool) {
	if registry == nil {
		return SinkBackpressureStatus{}, false
	}
	sink = strings.TrimSpace(sink)
	if sink == "" {
		return SinkBackpressureStatus{}, false
	}
	registry.mu.RLock()
	state, exists := registry.sinks[sink]
	if !exists {
		registry.mu.RUnlock()
		return SinkBackpressureStatus{}, false
	}
	status := state.status
	registry.mu.RUnlock()
	return status, true
}

// Snapshot returns all sink statuses sorted by sink name.
func (registry *SinkBackpressureRegistry) Snapshot() []SinkBackpressureStatus {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	snapshot := make([]SinkBackpressureStatus, 0, len(registry.sinks))
	for _, state := range registry.sinks {
		snapshot = append(snapshot, state.status)
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool { return snapshot[left].Sink < snapshot[right].Sink })
	return snapshot
}

// Len returns the number of registered sinks.
func (registry *SinkBackpressureRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	length := len(registry.sinks)
	registry.mu.RUnlock()
	return length
}

// Close wakes blocked waiters and rejects future operations. It is idempotent.
func (registry *SinkBackpressureRegistry) Close() error {
	if registry == nil {
		return ErrSinkBackpressureRegistryNil
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil
	}
	registry.closed = true
	for _, state := range registry.sinks {
		close(state.notify)
	}
	registry.mu.Unlock()
	return nil
}

func (registry *SinkBackpressureRegistry) refreshSinkLocked(state *sinkBackpressureState) {
	state.status.Lag = state.status.EmittedFrontier - state.status.AcknowledgedFrontier
	previousBlocked := state.status.Blocked
	if previousBlocked {
		if state.status.Lag <= state.status.LowWatermark {
			state.status.Blocked = false
		}
	} else if state.status.Lag >= state.status.HighWatermark {
		state.status.Blocked = true
	}
	state.status.UpdatedAt = time.Now().UTC()
	if previousBlocked != state.status.Blocked {
		close(state.notify)
		state.notify = make(chan struct{})
	}
}

func normalizeSinkBackpressureName(sink string) (string, error) {
	sink = strings.TrimSpace(sink)
	if sink == "" {
		return "", ErrSinkBackpressureNameRequired
	}
	return sink, nil
}

func normalizeSinkBackpressureWatermarks(high, low, defaultHigh, defaultLow uint64) (uint64, uint64, error) {
	if high == 0 {
		high = defaultHigh
		if low == 0 {
			low = defaultLow
		}
	}
	if low == 0 {
		low = high / 2
	}
	if err := validateSinkBackpressureWatermarks(high, low); err != nil {
		return 0, 0, err
	}
	return high, low, nil
}

func validateSinkBackpressureWatermarks(high, low uint64) error {
	if high == 0 || high > MaxSinkBackpressureWatermark || low >= high || low > MaxSinkBackpressureWatermark {
		return ErrSinkBackpressureOptionsInvalid
	}
	return nil
}
