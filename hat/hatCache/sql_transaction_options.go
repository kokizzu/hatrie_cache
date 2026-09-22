package hatCache

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrSQLTransactionReadOnly is returned when a mutation is attempted through
// a transaction opened with ReadOnly enabled.
var ErrSQLTransactionReadOnly = errors.New("SQL transaction is read-only")

// ErrSQLTransactionTimeout is returned when a transaction's configured wall
// clock timeout expires before an operation can complete.
var ErrSQLTransactionTimeout = errors.New("SQL transaction timed out")

// ErrSQLTransactionConflict is returned when a transaction observes a live
// mutation after its snapshot was captured.
var ErrSQLTransactionConflict = errors.New("SQL transaction conflict")

// SQLTransactionIsolation controls how a SQLTransaction coordinates with
// concurrent command-path mutations.
type SQLTransactionIsolation uint8

const (
	// SQLTransactionIsolationSnapshot retains the existing optimistic snapshot
	// behavior and is the zero-value default.
	SQLTransactionIsolationSnapshot SQLTransactionIsolation = iota
	// SQLTransactionIsolationSerializable holds the command transaction lock
	// from begin until commit or rollback.
	SQLTransactionIsolationSerializable
)

// DefaultSQLTransactionIsolation is used when SQLTransactionOptions.Isolation
// is left at its zero value.
const DefaultSQLTransactionIsolation = SQLTransactionIsolationSnapshot

// String returns the stable configuration spelling for an isolation level.
func (isolation SQLTransactionIsolation) String() string {
	switch isolation {
	case SQLTransactionIsolationSnapshot:
		return "snapshot"
	case SQLTransactionIsolationSerializable:
		return "serializable"
	default:
		return "unknown"
	}
}

// ParseSQLTransactionIsolation parses snapshot or serializable. An empty
// value selects the backward-compatible snapshot default.
func ParseSQLTransactionIsolation(value string) (SQLTransactionIsolation, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "snapshot":
		return SQLTransactionIsolationSnapshot, nil
	case "serializable":
		return SQLTransactionIsolationSerializable, nil
	default:
		return SQLTransactionIsolationSnapshot, fmt.Errorf("unsupported SQL transaction isolation %q", value)
	}
}

// SQLTransactionOptions configures BeginSQLTransactionWithOptions.
type SQLTransactionOptions struct {
	Isolation SQLTransactionIsolation
	// ReadOnly rejects Execute mutations while retaining snapshot reads. It is
	// disabled by the zero value for backward compatibility.
	ReadOnly bool
	// Timeout bounds the transaction after its private snapshot is captured. A
	// zero value disables the timeout and preserves the default fast path.
	Timeout time.Duration
	// EarlyConflictDetection aborts the transaction when a live mutation is
	// observed before or after a staged mutation. It is disabled by the zero
	// value so the existing commit-time conflict behavior remains unchanged.
	EarlyConflictDetection bool
}

func (options SQLTransactionOptions) normalized() (SQLTransactionOptions, error) {
	if options.Isolation > SQLTransactionIsolationSerializable {
		return SQLTransactionOptions{}, fmt.Errorf("unsupported SQL transaction isolation %d", options.Isolation)
	}
	if options.Timeout < 0 {
		return SQLTransactionOptions{}, fmt.Errorf("SQL transaction timeout cannot be negative")
	}
	return options, nil
}
