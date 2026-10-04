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

// ErrSQLTransactionJournalRequired is returned when journal durability is
// selected without a command journal.
var ErrSQLTransactionJournalRequired = errors.New("SQL transaction journal durability requires a command journal")

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

// SQLTransactionDurability controls how a transaction publishes writes.
type SQLTransactionDurability uint8

const (
	// SQLTransactionDurabilityMemory preserves the existing in-memory commit
	// path and is the zero-value default.
	SQLTransactionDurabilityMemory SQLTransactionDurability = iota
	// SQLTransactionDurabilityJournal appends and syncs the transaction as one
	// atomic journal batch before reporting commit success.
	SQLTransactionDurabilityJournal
)

// DefaultSQLTransactionDurability is the backward-compatible in-memory mode.
const DefaultSQLTransactionDurability = SQLTransactionDurabilityMemory

// String returns the stable configuration spelling for a durability mode.
func (durability SQLTransactionDurability) String() string {
	switch durability {
	case SQLTransactionDurabilityMemory:
		return "memory"
	case SQLTransactionDurabilityJournal:
		return "journal"
	default:
		return "unknown"
	}
}

// ParseSQLTransactionDurability parses memory or journal. An empty value
// selects the backward-compatible in-memory default.
func ParseSQLTransactionDurability(value string) (SQLTransactionDurability, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "memory", "default":
		return SQLTransactionDurabilityMemory, nil
	case "journal":
		return SQLTransactionDurabilityJournal, nil
	default:
		return SQLTransactionDurabilityMemory, fmt.Errorf("unsupported SQL transaction durability %q", value)
	}
}

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
	// Durability selects the commit publication path. Journal durability
	// requires Journal and is opt-in; memory is the zero-value default.
	Durability SQLTransactionDurability
	// Journal supplies the command journal used by journal durability.
	Journal *CommandJournal
}

func (options SQLTransactionOptions) normalized() (SQLTransactionOptions, error) {
	if options.Isolation > SQLTransactionIsolationSerializable {
		return SQLTransactionOptions{}, fmt.Errorf("unsupported SQL transaction isolation %d", options.Isolation)
	}
	if options.Timeout < 0 {
		return SQLTransactionOptions{}, fmt.Errorf("SQL transaction timeout cannot be negative")
	}
	if options.Durability > SQLTransactionDurabilityJournal {
		return SQLTransactionOptions{}, fmt.Errorf("unsupported SQL transaction durability %d", options.Durability)
	}
	if options.Durability == SQLTransactionDurabilityJournal && options.Journal == nil {
		return SQLTransactionOptions{}, ErrSQLTransactionJournalRequired
	}
	if options.Durability != SQLTransactionDurabilityJournal && options.Journal != nil {
		return SQLTransactionOptions{}, fmt.Errorf("SQL transaction journal is configured but durability is %s", options.Durability)
	}
	return options, nil
}
