package hatDataStructure

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	ErrLogicalFrontierNil       = errors.New("logical frontier is nil")
	ErrLogicalFrontierFull      = errors.New("logical frontier source limit reached")
	ErrLogicalSourceExists      = errors.New("logical frontier source already exists")
	ErrLogicalSourceNotFound    = errors.New("logical frontier source not found")
	ErrLogicalSourceNameInvalid = errors.New("logical frontier source name is invalid")
	ErrLogicalTimeRegression    = errors.New("logical time moved backwards")
	ErrLogicalFrontierConfig    = errors.New("logical frontier configuration is invalid")
)

const (
	defaultLogicalFrontierMaxSources  = 1024
	defaultLogicalFrontierNameBytes   = 256
	maxLogicalFrontierSources         = 65536
	maxLogicalFrontierSourceNameBytes = 4096
)

// LogicalFrontierConfig bounds the number and size of tracked input sources.
// Zero values select the defaults.
type LogicalFrontierConfig struct {
	MaxSources         int
	MaxSourceNameBytes int
}

// DefaultLogicalFrontierConfig returns conservative bounds for an opt-in
// frontier used by incremental or event-time processing.
func DefaultLogicalFrontierConfig() LogicalFrontierConfig {
	return LogicalFrontierConfig{
		MaxSources:         defaultLogicalFrontierMaxSources,
		MaxSourceNameBytes: defaultLogicalFrontierNameBytes,
	}
}

// LogicalSourceProgress is one source's latest observed logical timestamp.
type LogicalSourceProgress struct {
	Name      string
	Timestamp uint64
}

// LogicalFrontierSnapshot is a deterministic copy of frontier state.
// A frontier with no sources has HasSources=false and is considered complete
// by Wait because there are no unfinished inputs.
type LogicalFrontierSnapshot struct {
	Frontier   uint64
	HasSources bool
	Sources    []LogicalSourceProgress
}

// LogicalFrontier tracks the minimum timestamp among registered sources.
// It is a scalar, bounded primitive rather than a full multidimensional
// antichain implementation.
type LogicalFrontier struct {
	mu                 sync.Mutex
	maxSources         int
	maxSourceNameBytes int
	sources            map[string]LogicalSourceProgress
	frontier           uint64
	hasSources         bool
	changed            chan struct{}
}

// NewLogicalFrontier creates an empty bounded frontier.
func NewLogicalFrontier(config LogicalFrontierConfig) (*LogicalFrontier, error) {
	if config.MaxSources == 0 {
		config.MaxSources = defaultLogicalFrontierMaxSources
	}
	if config.MaxSourceNameBytes == 0 {
		config.MaxSourceNameBytes = defaultLogicalFrontierNameBytes
	}
	if config.MaxSources < 1 || config.MaxSources > maxLogicalFrontierSources ||
		config.MaxSourceNameBytes < 1 || config.MaxSourceNameBytes > maxLogicalFrontierSourceNameBytes {
		return nil, ErrLogicalFrontierConfig
	}
	return &LogicalFrontier{
		maxSources:         config.MaxSources,
		maxSourceNameBytes: config.MaxSourceNameBytes,
		changed:            make(chan struct{}),
	}, nil
}

// Register adds a source at its current logical timestamp.
func (f *LogicalFrontier) Register(name string, timestamp uint64) error {
	if f == nil {
		return ErrLogicalFrontierNil
	}
	if err := f.validateName(name); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.sources[name]; exists {
		return ErrLogicalSourceExists
	}
	if len(f.sources) >= f.maxSources {
		return ErrLogicalFrontierFull
	}
	if f.sources == nil {
		f.sources = make(map[string]LogicalSourceProgress, minInt(f.maxSources, 16))
	}
	f.sources[name] = LogicalSourceProgress{Name: name, Timestamp: timestamp}
	if !f.hasSources || timestamp < f.frontier {
		f.frontier = timestamp
		f.hasSources = true
		f.signalChangedLocked()
		return nil
	}
	f.hasSources = true
	return nil
}

// Advance moves one source forward. Equal timestamps are accepted without a
// notification; regressions are rejected to keep the frontier monotonic.
func (f *LogicalFrontier) Advance(name string, timestamp uint64) (bool, error) {
	if f == nil {
		return false, ErrLogicalFrontierNil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	source, exists := f.sources[name]
	if !exists {
		return false, ErrLogicalSourceNotFound
	}
	if timestamp < source.Timestamp {
		return false, ErrLogicalTimeRegression
	}
	if timestamp == source.Timestamp {
		return false, nil
	}
	oldFrontier := f.frontier
	oldTimestamp := source.Timestamp
	source.Timestamp = timestamp
	f.sources[name] = source
	if oldTimestamp == oldFrontier {
		f.recomputeFrontierLocked()
	}
	if f.frontier != oldFrontier {
		f.signalChangedLocked()
	}
	return true, nil
}

// Unregister removes a source and wakes frontier waiters.
func (f *LogicalFrontier) Unregister(name string) error {
	if f == nil {
		return ErrLogicalFrontierNil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.sources[name]; !exists {
		return ErrLogicalSourceNotFound
	}
	delete(f.sources, name)
	f.recomputeFrontierLocked()
	f.signalChangedLocked()
	return nil
}

// Current returns the minimum source timestamp and whether at least one source
// is registered. An empty frontier is complete for Wait but has no timestamp.
func (f *LogicalFrontier) Current() (timestamp uint64, hasSources bool) {
	if f == nil {
		return 0, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frontier, f.hasSources
}

// Snapshot returns an isolated, name-sorted copy of the frontier.
func (f *LogicalFrontier) Snapshot() LogicalFrontierSnapshot {
	if f == nil {
		return LogicalFrontierSnapshot{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := LogicalFrontierSnapshot{
		Frontier:   f.frontier,
		HasSources: f.hasSources,
		Sources:    make([]LogicalSourceProgress, 0, len(f.sources)),
	}
	for _, source := range f.sources {
		snapshot.Sources = append(snapshot.Sources, source)
	}
	sort.Slice(snapshot.Sources, func(i, j int) bool {
		return snapshot.Sources[i].Name < snapshot.Sources[j].Name
	})
	return snapshot
}

// Wait blocks until the frontier reaches target or the context is canceled.
// It never starts a goroutine per waiter.
func (f *LogicalFrontier) Wait(ctx context.Context, target uint64) error {
	if f == nil {
		return ErrLogicalFrontierNil
	}
	if ctx == nil {
		return context.Canceled
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		f.mu.Lock()
		ready := !f.hasSources || f.frontier >= target
		changed := f.changed
		f.mu.Unlock()
		if ready {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (f *LogicalFrontier) validateName(name string) error {
	if name == "" || len(name) > f.maxSourceNameBytes {
		return ErrLogicalSourceNameInvalid
	}
	return nil
}

func (f *LogicalFrontier) recomputeFrontierLocked() {
	if len(f.sources) == 0 {
		f.frontier = 0
		f.hasSources = false
		return
	}
	var minimum uint64
	first := true
	for _, source := range f.sources {
		if first || source.Timestamp < minimum {
			minimum = source.Timestamp
			first = false
		}
	}
	f.frontier = minimum
	f.hasSources = true
}

func (f *LogicalFrontier) signalChangedLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
