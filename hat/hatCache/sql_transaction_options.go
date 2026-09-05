package hatCache

import (
	"fmt"
	"strings"
)

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
}

func (options SQLTransactionOptions) normalized() (SQLTransactionOptions, error) {
	if options.Isolation > SQLTransactionIsolationSerializable {
		return SQLTransactionOptions{}, fmt.Errorf("unsupported SQL transaction isolation %d", options.Isolation)
	}
	return options, nil
}
