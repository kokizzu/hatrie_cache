package hatSql

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrSQLSessionTransactionSettingsInvalid reports an unsupported session
	// transaction setting or a negative timeout.
	ErrSQLSessionTransactionSettingsInvalid = errors.New("hatSql: invalid SQL session transaction settings")
	// ErrSQLSessionReadOnly reports a session-local mutation rejected by a
	// read-only session.
	ErrSQLSessionReadOnly = errors.New("hatSql: SQL session is read-only")
	// ErrSQLSessionNil reports a method call on a nil session.
	ErrSQLSessionNil = errors.New("hatSql: SQL session is nil")
)

// SQLSessionTransactionIsolation names the isolation contract an embedding
// transaction adapter should use for a session.
type SQLSessionTransactionIsolation string

const (
	SQLSessionTransactionIsolationDefault        SQLSessionTransactionIsolation = ""
	SQLSessionTransactionIsolationReadCommitted  SQLSessionTransactionIsolation = "read_committed"
	SQLSessionTransactionIsolationRepeatableRead SQLSessionTransactionIsolation = "repeatable_read"
	SQLSessionTransactionIsolationSerializable   SQLSessionTransactionIsolation = "serializable"
)

// SQLSessionTransactionDurability names the commit durability contract an
// embedding transaction adapter should use for a session.
type SQLSessionTransactionDurability string

const (
	SQLSessionTransactionDurabilityDefault   SQLSessionTransactionDurability = ""
	SQLSessionTransactionDurabilityBatch     SQLSessionTransactionDurability = "batch"
	SQLSessionTransactionDurabilityImmediate SQLSessionTransactionDurability = "immediate"
	SQLSessionTransactionDurabilityDisabled  SQLSessionTransactionDurability = "disabled"
)

// SQLSessionTransactionSettings are inherited by one SQLSession. Timeout and
// ReadOnly are enforced by SQLSession itself; Isolation and Durability are
// validated metadata for a caller-owned transaction adapter.
type SQLSessionTransactionSettings struct {
	Isolation  SQLSessionTransactionIsolation
	Timeout    time.Duration
	ReadOnly   bool
	Durability SQLSessionTransactionDurability
}

func (settings SQLSessionTransactionSettings) validate() error {
	if settings.Timeout < 0 {
		return ErrSQLSessionTransactionSettingsInvalid
	}
	switch settings.Isolation {
	case SQLSessionTransactionIsolationDefault,
		SQLSessionTransactionIsolationReadCommitted,
		SQLSessionTransactionIsolationRepeatableRead,
		SQLSessionTransactionIsolationSerializable:
	default:
		return ErrSQLSessionTransactionSettingsInvalid
	}
	switch settings.Durability {
	case SQLSessionTransactionDurabilityDefault,
		SQLSessionTransactionDurabilityBatch,
		SQLSessionTransactionDurabilityImmediate,
		SQLSessionTransactionDurabilityDisabled:
	default:
		return ErrSQLSessionTransactionSettingsInvalid
	}
	return nil
}

// WithContext derives the per-session deadline without replacing a caller's
// earlier deadline. A zero timeout preserves the caller context and allocates
// no timer.
func (settings SQLSessionTransactionSettings) WithContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if settings.Timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, settings.Timeout)
}

// NewSQLSessionWithTransactionSettings creates a session with validated
// inherited settings. NewSQLSession remains the zero-settings compatibility
// constructor.
func NewSQLSessionWithTransactionSettings(source SourceResolver, settings SQLSessionTransactionSettings) (*SQLSession, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	session := NewSQLSession(source)
	session.transactionSettings.Store(&settings)
	return session, nil
}

// TransactionSettings returns an isolated immutable snapshot of the current
// session settings.
func (session *SQLSession) TransactionSettings() SQLSessionTransactionSettings {
	if session == nil {
		return SQLSessionTransactionSettings{}
	}
	settings := session.transactionSettings.Load()
	if settings == nil {
		return SQLSessionTransactionSettings{}
	}
	return *settings
}

// SetTransactionSettings replaces the session settings atomically. Existing
// in-flight queries retain the context they already derived.
func (session *SQLSession) SetTransactionSettings(settings SQLSessionTransactionSettings) error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if err := settings.validate(); err != nil {
		return err
	}
	session.transactionSettings.Store(&settings)
	return nil
}

// ResetTransactionSettings restores the compatibility defaults.
func (session *SQLSession) ResetTransactionSettings() error {
	return session.SetTransactionSettings(SQLSessionTransactionSettings{})
}

func (session *SQLSession) transactionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return session.TransactionSettings().WithContext(ctx)
}

func (session *SQLSession) ensureSessionMutationAllowed() error {
	if session == nil {
		return ErrSQLSessionNil
	}
	if session.TransactionSettings().ReadOnly {
		return ErrSQLSessionReadOnly
	}
	return nil
}
