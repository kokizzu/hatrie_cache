package hatSql

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// MaxSQLSessionTransactionTimeout bounds caller-controlled session query
	// deadlines. Zero means no session-imposed deadline.
	MaxSQLSessionTransactionTimeout = 24 * time.Hour
)

var (
	// ErrSQLSessionTransactionSettingsInvalid reports an unsupported isolation,
	// durability, or timeout setting.
	ErrSQLSessionTransactionSettingsInvalid = errors.New("hatSql: invalid SQL session transaction settings")
	// ErrSQLSessionReadOnly reports an attempted session mutation while the
	// session transaction is read-only.
	ErrSQLSessionReadOnly = errors.New("hatSql: SQL session transaction is read-only")
)

// SQLSessionIsolation is the isolation contract exposed to a session's
// caller-owned transaction engine. SQLSession itself is an in-memory query
// session and does not implement locking or MVCC for external sources.
type SQLSessionIsolation string

const (
	// SQLSessionIsolationReadCommitted is the low-overhead default contract.
	SQLSessionIsolationReadCommitted SQLSessionIsolation = "read_committed"
	// SQLSessionIsolationRepeatableRead requests a stable read contract from
	// the caller-owned transaction engine.
	SQLSessionIsolationRepeatableRead SQLSessionIsolation = "repeatable_read"
	// SQLSessionIsolationSerializable requests serializable execution from the
	// caller-owned transaction engine.
	SQLSessionIsolationSerializable SQLSessionIsolation = "serializable"
)

// SQLSessionDurability describes the caller-owned persistence contract for a
// session transaction. SQLSessionDurabilityMemory is the correct default for
// SQLSession's in-memory temporary state.
type SQLSessionDurability string

const (
	// SQLSessionDurabilityMemory keeps the session contract in memory.
	SQLSessionDurabilityMemory SQLSessionDurability = "memory"
	// SQLSessionDurabilityDurable asks the caller-owned transaction boundary to
	// persist its effects before acknowledging commit.
	SQLSessionDurabilityDurable SQLSessionDurability = "durable"
)

// SQLSessionTransactionSettings is the per-session transaction contract.
// Isolation and Durability are metadata for the transaction layer that owns
// external source writes; Timeout and ReadOnly are enforced by SQLSession.
type SQLSessionTransactionSettings struct {
	Isolation  SQLSessionIsolation
	Timeout    time.Duration
	ReadOnly   bool
	Durability SQLSessionDurability
}

var defaultSQLSessionTransactionSettings = SQLSessionTransactionSettings{
	Isolation:  SQLSessionIsolationReadCommitted,
	Durability: SQLSessionDurabilityMemory,
}

// TransactionSettings returns an independent copy of the session contract.
// A nil session returns the safe default.
func (session *SQLSession) TransactionSettings() SQLSessionTransactionSettings {
	if session == nil {
		return defaultSQLSessionTransactionSettings
	}
	session.mu.RLock()
	settings := session.transactionSettings
	session.mu.RUnlock()
	return settings
}

// SetTransactionSettings validates and atomically replaces the session
// contract. The default zero-valued Timeout preserves existing unbounded
// query behavior.
func (session *SQLSession) SetTransactionSettings(settings SQLSessionTransactionSettings) error {
	if session == nil {
		return fmt.Errorf("%w: nil session", ErrSQLSessionTransactionSettingsInvalid)
	}
	normalized, err := normalizeSQLSessionTransactionSettings(settings)
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.transactionSettings = normalized
	session.mu.Unlock()
	return nil
}

// ResetTransactionSettings restores the safe in-memory, read-write,
// no-deadline defaults.
func (session *SQLSession) ResetTransactionSettings() {
	if session == nil {
		return
	}
	session.mu.Lock()
	session.transactionSettings = defaultSQLSessionTransactionSettings
	session.mu.Unlock()
}

func normalizeSQLSessionTransactionSettings(settings SQLSessionTransactionSettings) (SQLSessionTransactionSettings, error) {
	if settings.Isolation == "" {
		settings.Isolation = SQLSessionIsolationReadCommitted
	}
	if settings.Durability == "" {
		settings.Durability = SQLSessionDurabilityMemory
	}
	switch settings.Isolation {
	case SQLSessionIsolationReadCommitted, SQLSessionIsolationRepeatableRead, SQLSessionIsolationSerializable:
	default:
		return SQLSessionTransactionSettings{}, fmt.Errorf("%w: isolation %q", ErrSQLSessionTransactionSettingsInvalid, settings.Isolation)
	}
	switch settings.Durability {
	case SQLSessionDurabilityMemory, SQLSessionDurabilityDurable:
	default:
		return SQLSessionTransactionSettings{}, fmt.Errorf("%w: durability %q", ErrSQLSessionTransactionSettingsInvalid, settings.Durability)
	}
	if settings.Timeout < 0 || settings.Timeout > MaxSQLSessionTransactionTimeout {
		return SQLSessionTransactionSettings{}, fmt.Errorf("%w: timeout %s", ErrSQLSessionTransactionSettingsInvalid, settings.Timeout)
	}
	return settings, nil
}

func (session *SQLSession) transactionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if session == nil {
		return ctx, func() {}
	}
	settings := session.TransactionSettings()
	if settings.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, settings.Timeout)
}

func (session *SQLSession) transactionReadOnly() bool {
	return session != nil && session.TransactionSettings().ReadOnly
}

func (session *SQLSession) rejectReadOnlyMutation() error {
	if session.transactionReadOnly() {
		return ErrSQLSessionReadOnly
	}
	return nil
}
