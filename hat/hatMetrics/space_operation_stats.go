package hatMetrics

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	// MaxSpaceOperationStatsNameBytes bounds one space or index identity.
	MaxSpaceOperationStatsNameBytes = 256
	// MaxSpaceOperationStatsEntries bounds one registry's retained identities.
	MaxSpaceOperationStatsEntries = 1 << 16
)

var (
	// ErrSpaceOperationStatsInvalid indicates a nil metrics handle or malformed
	// identity.
	ErrSpaceOperationStatsInvalid = errors.New("hatMetrics: space operation stats are invalid")
	// ErrSpaceOperationStatsDuplicate indicates an identity already registered.
	ErrSpaceOperationStatsDuplicate = errors.New("hatMetrics: space operation stats name is already registered")
	// ErrSpaceOperationStatsNotFound indicates an unregistered identity.
	ErrSpaceOperationStatsNotFound = errors.New("hatMetrics: space operation stats name is not registered")
	// ErrSpaceOperationStatsLimit indicates that the registry is full.
	ErrSpaceOperationStatsLimit = errors.New("hatMetrics: space operation stats registry limit exceeded")
	// ErrSpaceOperationKindInvalid indicates an unsupported operation kind.
	ErrSpaceOperationKindInvalid = errors.New("hatMetrics: space operation kind is invalid")
	// ErrSpaceOperationOutcomeInvalid indicates an unsupported operation outcome.
	ErrSpaceOperationOutcomeInvalid = errors.New("hatMetrics: space operation outcome is invalid")
	// ErrSpaceOperationLatencyInvalid indicates a negative latency.
	ErrSpaceOperationLatencyInvalid = errors.New("hatMetrics: space operation latency is invalid")
)

// SpaceOperationKind identifies one operation family for a named space or
// secondary index.
type SpaceOperationKind uint8

const (
	SpaceOperationInvalid SpaceOperationKind = iota
	SpaceOperationRead
	SpaceOperationWrite
	SpaceOperationDelete
	SpaceOperationScan
)

// String returns the stable operation name.
func (kind SpaceOperationKind) String() string {
	switch kind {
	case SpaceOperationRead:
		return "read"
	case SpaceOperationWrite:
		return "write"
	case SpaceOperationDelete:
		return "delete"
	case SpaceOperationScan:
		return "scan"
	default:
		return "invalid"
	}
}

// SpaceOperationOutcome identifies the result of one recorded operation.
// Success does not affect hit, miss, or error counters.
type SpaceOperationOutcome uint8

const (
	SpaceOperationOutcomeInvalid SpaceOperationOutcome = iota
	SpaceOperationSuccess
	SpaceOperationHit
	SpaceOperationMiss
	SpaceOperationError
)

// String returns the stable outcome name.
func (outcome SpaceOperationOutcome) String() string {
	switch outcome {
	case SpaceOperationSuccess:
		return "success"
	case SpaceOperationHit:
		return "hit"
	case SpaceOperationMiss:
		return "miss"
	case SpaceOperationError:
		return "error"
	default:
		return "invalid"
	}
}

// SpaceOperationSnapshot is a point-in-time report for one named space or
// index. LatencyNanos is the sum of the supplied durations, not a percentile.
type SpaceOperationSnapshot struct {
	Name             string `json:"name"`
	Operations       uint64 `json:"operations"`
	ReadOperations   uint64 `json:"read_operations"`
	WriteOperations  uint64 `json:"write_operations"`
	DeleteOperations uint64 `json:"delete_operations"`
	ScanOperations   uint64 `json:"scan_operations"`
	Hits             uint64 `json:"hits"`
	Misses           uint64 `json:"misses"`
	Errors           uint64 `json:"errors"`
	BytesRead        uint64 `json:"bytes_read"`
	BytesWritten     uint64 `json:"bytes_written"`
	LatencyNanos     uint64 `json:"latency_nanos"`
	LatencySamples   uint64 `json:"latency_samples"`
}

// SpaceOperationMetrics is an allocation-free atomic counter handle for one
// named space or index. The name is immutable and the handle is safe for
// concurrent Record and Snapshot calls.
type SpaceOperationMetrics struct {
	name             string
	operations       atomic.Uint64
	readOperations   atomic.Uint64
	writeOperations  atomic.Uint64
	deleteOperations atomic.Uint64
	scanOperations   atomic.Uint64
	hits             atomic.Uint64
	misses           atomic.Uint64
	errors           atomic.Uint64
	bytesRead        atomic.Uint64
	bytesWritten     atomic.Uint64
	latencyNanos     atomic.Uint64
	latencySamples   atomic.Uint64
}

// NewSpaceOperationMetrics creates an independent counter handle. It is
// outside any registry and is useful when the caller already owns the space
// lifecycle.
func NewSpaceOperationMetrics(name string) (*SpaceOperationMetrics, error) {
	name, err := normalizeSpaceOperationStatsName(name)
	if err != nil {
		return nil, err
	}
	return &SpaceOperationMetrics{name: name}, nil
}

// Name returns the stable identity of the handle.
func (metrics *SpaceOperationMetrics) Name() string {
	if metrics == nil {
		return ""
	}
	return metrics.name
}

// Record adds one operation and its supplied byte and latency measurements.
// Read and scan bytes contribute to BytesRead; write and delete bytes
// contribute to BytesWritten. The method performs no allocation on valid calls.
func (metrics *SpaceOperationMetrics) Record(kind SpaceOperationKind, outcome SpaceOperationOutcome, bytes uint64, latency time.Duration) error {
	if metrics == nil {
		return ErrSpaceOperationStatsInvalid
	}
	if kind < SpaceOperationRead || kind > SpaceOperationScan {
		return ErrSpaceOperationKindInvalid
	}
	if outcome < SpaceOperationSuccess || outcome > SpaceOperationError {
		return ErrSpaceOperationOutcomeInvalid
	}
	if latency < 0 {
		return ErrSpaceOperationLatencyInvalid
	}

	metrics.operations.Add(1)
	switch kind {
	case SpaceOperationRead:
		metrics.readOperations.Add(1)
		metrics.bytesRead.Add(bytes)
	case SpaceOperationWrite:
		metrics.writeOperations.Add(1)
		metrics.bytesWritten.Add(bytes)
	case SpaceOperationDelete:
		metrics.deleteOperations.Add(1)
		metrics.bytesWritten.Add(bytes)
	case SpaceOperationScan:
		metrics.scanOperations.Add(1)
		metrics.bytesRead.Add(bytes)
	}
	switch outcome {
	case SpaceOperationHit:
		metrics.hits.Add(1)
	case SpaceOperationMiss:
		metrics.misses.Add(1)
	case SpaceOperationError:
		metrics.errors.Add(1)
	}
	metrics.latencyNanos.Add(uint64(latency))
	metrics.latencySamples.Add(1)
	return nil
}

// Snapshot returns the current counter values. Individual counters are atomic;
// a concurrent Record may make fields originate from adjacent instants.
func (metrics *SpaceOperationMetrics) Snapshot() SpaceOperationSnapshot {
	if metrics == nil {
		return SpaceOperationSnapshot{}
	}
	return SpaceOperationSnapshot{
		Name:             metrics.name,
		Operations:       metrics.operations.Load(),
		ReadOperations:   metrics.readOperations.Load(),
		WriteOperations:  metrics.writeOperations.Load(),
		DeleteOperations: metrics.deleteOperations.Load(),
		ScanOperations:   metrics.scanOperations.Load(),
		Hits:             metrics.hits.Load(),
		Misses:           metrics.misses.Load(),
		Errors:           metrics.errors.Load(),
		BytesRead:        metrics.bytesRead.Load(),
		BytesWritten:     metrics.bytesWritten.Load(),
		LatencyNanos:     metrics.latencyNanos.Load(),
		LatencySamples:   metrics.latencySamples.Load(),
	}
}

// SpaceOperationStatsRegistry stores bounded metrics handles by name. It is
// safe for concurrent registration, lookup, reporting, and convenience Record
// calls. Retaining a handle from Lookup avoids the registry lock on hot paths.
type SpaceOperationStatsRegistry struct {
	mu      sync.RWMutex
	metrics map[string]*SpaceOperationMetrics
}

// NewSpaceOperationStatsRegistry creates an empty registry.
func NewSpaceOperationStatsRegistry() *SpaceOperationStatsRegistry {
	return &SpaceOperationStatsRegistry{metrics: make(map[string]*SpaceOperationMetrics)}
}

// Register creates and registers one named metrics handle. Names are trimmed
// before storage and duplicate names are rejected.
func (registry *SpaceOperationStatsRegistry) Register(name string) (*SpaceOperationMetrics, error) {
	name, err := normalizeSpaceOperationStatsName(name)
	if err != nil {
		return nil, err
	}
	if registry == nil {
		return nil, ErrSpaceOperationStatsInvalid
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.metrics == nil {
		registry.metrics = make(map[string]*SpaceOperationMetrics)
	}
	if _, exists := registry.metrics[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrSpaceOperationStatsDuplicate, name)
	}
	if len(registry.metrics) >= MaxSpaceOperationStatsEntries {
		return nil, ErrSpaceOperationStatsLimit
	}
	metrics := &SpaceOperationMetrics{name: name}
	registry.metrics[name] = metrics
	return metrics, nil
}

// Lookup returns the stable metrics handle for name.
func (registry *SpaceOperationStatsRegistry) Lookup(name string) (*SpaceOperationMetrics, bool) {
	if registry == nil {
		return nil, false
	}
	name = strings.TrimSpace(name)
	registry.mu.RLock()
	metrics, ok := registry.metrics[name]
	registry.mu.RUnlock()
	return metrics, ok
}

// Record looks up name and records one operation. Prefer Lookup plus the
// returned handle for repeated operations.
func (registry *SpaceOperationStatsRegistry) Record(name string, kind SpaceOperationKind, outcome SpaceOperationOutcome, bytes uint64, latency time.Duration) error {
	metrics, ok := registry.Lookup(name)
	if !ok {
		return ErrSpaceOperationStatsNotFound
	}
	return metrics.Record(kind, outcome, bytes, latency)
}

// Snapshot returns independently owned reports sorted by name.
func (registry *SpaceOperationStatsRegistry) Snapshot() []SpaceOperationSnapshot {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.metrics))
	for name := range registry.metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]SpaceOperationSnapshot, 0, len(names))
	for _, name := range names {
		rows = append(rows, registry.metrics[name].Snapshot())
	}
	registry.mu.RUnlock()
	return rows
}

func normalizeSpaceOperationStatsName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > MaxSpaceOperationStatsNameBytes || !utf8.ValidString(name) {
		return "", ErrSpaceOperationStatsInvalid
	}
	return name, nil
}
