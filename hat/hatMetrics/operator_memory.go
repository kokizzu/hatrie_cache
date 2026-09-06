package hatMetrics

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var ErrOperatorNameRequired = errors.New("hatriecache: operator name is required")

// OperatorMemory is a point-in-time retained-memory gauge for one operator.
type OperatorMemory struct {
	Operator      string `json:"operator"`
	RetainedBytes uint64 `json:"retained_bytes"`
}

// OperatorMemoryRegistry stores the latest retained-memory gauge for each
// operator. It is safe for concurrent reporters and snapshot readers.
type OperatorMemoryRegistry struct {
	mu       sync.RWMutex
	retained map[string]uint64
}

// NewOperatorMemoryRegistry creates an empty operator memory registry.
func NewOperatorMemoryRegistry() *OperatorMemoryRegistry {
	return &OperatorMemoryRegistry{retained: make(map[string]uint64)}
}

// Set records retained bytes for operator. The value is a gauge and may move
// in either direction as the operator grows, compacts, or is released.
func (registry *OperatorMemoryRegistry) Set(operator string, retainedBytes uint64) error {
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return ErrOperatorNameRequired
	}
	if registry == nil {
		return errors.New("hatriecache: nil operator memory registry")
	}
	registry.mu.Lock()
	if registry.retained == nil {
		registry.retained = make(map[string]uint64)
	}
	registry.retained[operator] = retainedBytes
	registry.mu.Unlock()
	return nil
}

// Snapshot returns independently owned operator gauges sorted by operator
// name for deterministic export and comparison.
func (registry *OperatorMemoryRegistry) Snapshot() []OperatorMemory {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	operators := make([]string, 0, len(registry.retained))
	for operator := range registry.retained {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	rows := make([]OperatorMemory, 0, len(operators))
	for _, operator := range operators {
		rows = append(rows, OperatorMemory{Operator: operator, RetainedBytes: registry.retained[operator]})
	}
	registry.mu.RUnlock()
	return rows
}
