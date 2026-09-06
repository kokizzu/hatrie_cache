package hatMetrics

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var ErrReadAmplificationIdentityRequired = errors.New("hatriecache: read amplification part and column are required")

// ReadAmplification is a point-in-time read accounting total for one part and
// column.
type ReadAmplification struct {
	Part           string `json:"part"`
	Column         string `json:"column"`
	ReadOperations uint64 `json:"read_operations"`
	BytesRead      uint64 `json:"bytes_read"`
	BytesReturned  uint64 `json:"bytes_returned"`
}

// Ratio returns bytes read divided by bytes returned. It returns zero when no
// bytes have been returned, which keeps empty or metadata-only reads safe to
// report.
func (amplification ReadAmplification) Ratio() float64 {
	if amplification.BytesReturned == 0 {
		return 0
	}
	return float64(amplification.BytesRead) / float64(amplification.BytesReturned)
}

type readAmplificationKey struct {
	part   string
	column string
}

// ReadAmplificationRegistry aggregates read bytes for individual part and
// column identities. It is safe for concurrent reporters and snapshot
// readers.
type ReadAmplificationRegistry struct {
	mu    sync.RWMutex
	reads map[readAmplificationKey]ReadAmplification
}

// NewReadAmplificationRegistry creates an empty read accounting registry.
func NewReadAmplificationRegistry() *ReadAmplificationRegistry {
	return &ReadAmplificationRegistry{reads: make(map[readAmplificationKey]ReadAmplification)}
}

// Record adds one read observation. Part and column names are trimmed before
// they are used as the identity. Nil registries intentionally discard the
// observation so instrumentation cannot make a data path fail.
func (registry *ReadAmplificationRegistry) Record(part, column string, bytesRead, bytesReturned uint64) error {
	part = strings.TrimSpace(part)
	column = strings.TrimSpace(column)
	if part == "" || column == "" {
		return ErrReadAmplificationIdentityRequired
	}
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	if registry.reads == nil {
		registry.reads = make(map[readAmplificationKey]ReadAmplification)
	}
	key := readAmplificationKey{part: part, column: column}
	amplification := registry.reads[key]
	amplification.Part = part
	amplification.Column = column
	amplification.ReadOperations++
	amplification.BytesRead += bytesRead
	amplification.BytesReturned += bytesReturned
	registry.reads[key] = amplification
	registry.mu.Unlock()
	return nil
}

// Snapshot returns independently owned rows sorted by part and column for
// deterministic export and comparison.
func (registry *ReadAmplificationRegistry) Snapshot() []ReadAmplification {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	if len(registry.reads) == 0 {
		registry.mu.RUnlock()
		return nil
	}
	keys := make([]readAmplificationKey, 0, len(registry.reads))
	for key := range registry.reads {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].part == keys[right].part {
			return keys[left].column < keys[right].column
		}
		return keys[left].part < keys[right].part
	})
	rows := make([]ReadAmplification, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, registry.reads[key])
	}
	registry.mu.RUnlock()
	return rows
}
