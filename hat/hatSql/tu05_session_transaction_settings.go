package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	// ErrSQLTransactionSettingsInvalid reports an invalid isolation, timeout,
	// durability, or patch value.
	ErrSQLTransactionSettingsInvalid = errors.New("SQL transaction settings are invalid")
	// ErrSQLTransactionSettingsScopeClosed reports a second completion attempt.
	ErrSQLTransactionSettingsScopeClosed = errors.New("SQL transaction settings scope is closed")
	// ErrSQLTransactionSettingsNil reports a nil session or scope receiver.
	ErrSQLTransactionSettingsNil = errors.New("SQL transaction settings receiver is nil")
)

// SQLTransactionIsolation is the caller-visible isolation policy for one
// transaction. The executor remains responsible for enforcing the policy.
type SQLTransactionIsolation uint8

const (
	// SQLTransactionIsolationDefault inherits the session default or normalizes
	// to read committed in a complete settings value.
	SQLTransactionIsolationDefault SQLTransactionIsolation = iota
	SQLTransactionIsolationReadCommitted
	SQLTransactionIsolationRepeatableRead
	SQLTransactionIsolationSerializable
)

// SQLTransactionDurability is the requested write durability policy. The
// default is durable; relaxed and volatile modes require an explicit caller
// choice and do not alter any existing WAL behavior by themselves.
type SQLTransactionDurability uint8

const (
	// SQLTransactionDurabilityDefault inherits the session default or
	// normalizes to durable in a complete settings value.
	SQLTransactionDurabilityDefault SQLTransactionDurability = iota
	SQLTransactionDurabilityDurable
	SQLTransactionDurabilityRelaxed
	SQLTransactionDurabilityVolatile
)

// SQLTransactionSettings is a complete, immutable-by-convention transaction
// policy snapshot. Timeout zero means that the caller's context supplies the
// deadline. ReadOnly is independent of durability.
type SQLTransactionSettings struct {
	Isolation  SQLTransactionIsolation
	ReadOnly   bool
	Timeout    time.Duration
	Durability SQLTransactionDurability
}

// DefaultSQLTransactionSettings returns the safe compatibility defaults.
func DefaultSQLTransactionSettings() SQLTransactionSettings {
	return SQLTransactionSettings{
		Isolation:  SQLTransactionIsolationReadCommitted,
		Durability: SQLTransactionDurabilityDurable,
	}
}

// Normalize validates a complete settings value and fills its safe defaults.
func (settings SQLTransactionSettings) Normalize() (SQLTransactionSettings, error) {
	if settings.Timeout < 0 {
		return SQLTransactionSettings{}, fmt.Errorf("%w: timeout cannot be negative", ErrSQLTransactionSettingsInvalid)
	}
	switch settings.Isolation {
	case SQLTransactionIsolationDefault:
		settings.Isolation = SQLTransactionIsolationReadCommitted
	case SQLTransactionIsolationReadCommitted, SQLTransactionIsolationRepeatableRead, SQLTransactionIsolationSerializable:
	default:
		return SQLTransactionSettings{}, fmt.Errorf("%w: unknown isolation %d", ErrSQLTransactionSettingsInvalid, settings.Isolation)
	}
	switch settings.Durability {
	case SQLTransactionDurabilityDefault:
		settings.Durability = SQLTransactionDurabilityDurable
	case SQLTransactionDurabilityDurable, SQLTransactionDurabilityRelaxed, SQLTransactionDurabilityVolatile:
	default:
		return SQLTransactionSettings{}, fmt.Errorf("%w: unknown durability %d", ErrSQLTransactionSettingsInvalid, settings.Durability)
	}
	return settings, nil
}

// SQLTransactionSettingsPatch contains optional per-transaction overrides.
// The Set booleans distinguish an explicit false/zero value from inheritance.
type SQLTransactionSettingsPatch struct {
	Isolation   SQLTransactionIsolation
	ReadOnly    bool
	ReadOnlySet bool
	Timeout     time.Duration
	TimeoutSet  bool
	Durability  SQLTransactionDurability
}

// Apply resolves a patch against complete session defaults.
func (settings SQLTransactionSettings) Apply(patch SQLTransactionSettingsPatch) (SQLTransactionSettings, error) {
	settings, err := settings.Normalize()
	if err != nil {
		return SQLTransactionSettings{}, err
	}
	if patch.Isolation != SQLTransactionIsolationDefault {
		if patch.Isolation != SQLTransactionIsolationReadCommitted && patch.Isolation != SQLTransactionIsolationRepeatableRead && patch.Isolation != SQLTransactionIsolationSerializable {
			return SQLTransactionSettings{}, fmt.Errorf("%w: unknown isolation override %d", ErrSQLTransactionSettingsInvalid, patch.Isolation)
		}
		settings.Isolation = patch.Isolation
	}
	if patch.ReadOnlySet {
		settings.ReadOnly = patch.ReadOnly
	}
	if patch.TimeoutSet {
		if patch.Timeout < 0 {
			return SQLTransactionSettings{}, fmt.Errorf("%w: timeout override cannot be negative", ErrSQLTransactionSettingsInvalid)
		}
		settings.Timeout = patch.Timeout
	}
	if patch.Durability != SQLTransactionDurabilityDefault {
		if patch.Durability != SQLTransactionDurabilityDurable && patch.Durability != SQLTransactionDurabilityRelaxed && patch.Durability != SQLTransactionDurabilityVolatile {
			return SQLTransactionSettings{}, fmt.Errorf("%w: unknown durability override %d", ErrSQLTransactionSettingsInvalid, patch.Durability)
		}
		settings.Durability = patch.Durability
	}
	return settings, nil
}

type sqlTransactionSettingsSnapshot struct {
	settings SQLTransactionSettings
}

// SQLTransactionSettingsSession stores process-local session defaults with
// copy-on-write updates and allocation-free reads. It is safe for concurrent
// setting updates and transaction starts.
type SQLTransactionSettingsSession struct {
	defaults atomic.Pointer[sqlTransactionSettingsSnapshot]
}

// NewSQLTransactionSettingsSession creates a session with normalized defaults.
func NewSQLTransactionSettingsSession(settings SQLTransactionSettings) (*SQLTransactionSettingsSession, error) {
	normalized, err := settings.Normalize()
	if err != nil {
		return nil, err
	}
	session := &SQLTransactionSettingsSession{}
	session.defaults.Store(&sqlTransactionSettingsSnapshot{settings: normalized})
	return session, nil
}

// Defaults returns a complete copy of the current session defaults. A nil
// session returns the safe defaults so read-only inspection remains harmless.
func (session *SQLTransactionSettingsSession) Defaults() SQLTransactionSettings {
	if session == nil {
		return DefaultSQLTransactionSettings()
	}
	snapshot := session.defaults.Load()
	if snapshot == nil {
		return DefaultSQLTransactionSettings()
	}
	return snapshot.settings
}

// SetDefaults atomically publishes normalized session defaults. Existing
// scopes retain their captured settings.
func (session *SQLTransactionSettingsSession) SetDefaults(settings SQLTransactionSettings) error {
	if session == nil {
		return ErrSQLTransactionSettingsNil
	}
	normalized, err := settings.Normalize()
	if err != nil {
		return err
	}
	session.defaults.Store(&sqlTransactionSettingsSnapshot{settings: normalized})
	return nil
}

// ResetDefaults restores the safe compatibility defaults.
func (session *SQLTransactionSettingsSession) ResetDefaults() error {
	return session.SetDefaults(DefaultSQLTransactionSettings())
}

// Resolve applies a per-transaction patch to the current session snapshot.
func (session *SQLTransactionSettingsSession) Resolve(patch SQLTransactionSettingsPatch) (SQLTransactionSettings, error) {
	if session == nil {
		return SQLTransactionSettings{}, ErrSQLTransactionSettingsNil
	}
	return session.Defaults().Apply(patch)
}

// Begin captures settings and creates a context-bearing scope. This method
// does not mutate storage or commit a database transaction; callers pass the
// returned settings and context to their existing executor.
func (session *SQLTransactionSettingsSession) Begin(ctx context.Context, patch SQLTransactionSettingsPatch) (*SQLTransactionSettingsScope, error) {
	settings, err := session.Resolve(patch)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if settings.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, settings.Timeout)
	}
	return &SQLTransactionSettingsScope{settings: settings, ctx: ctx, cancel: cancel}, nil
}

// SQLTransactionSettingsScope is one captured settings snapshot and its
// optional timeout context. Commit and Rollback only close the scope; the
// caller owns the actual transaction operation.
type SQLTransactionSettingsScope struct {
	settings SQLTransactionSettings
	ctx      context.Context
	cancel   context.CancelFunc
	state    atomic.Uint32
}

// Settings returns the captured settings.
func (scope *SQLTransactionSettingsScope) Settings() SQLTransactionSettings {
	if scope == nil {
		return DefaultSQLTransactionSettings()
	}
	return scope.settings
}

// Context returns the caller context with the configured timeout applied.
func (scope *SQLTransactionSettingsScope) Context() context.Context {
	if scope == nil || scope.ctx == nil {
		return context.Background()
	}
	return scope.ctx
}

// Done reports whether the scope has been committed or rolled back.
func (scope *SQLTransactionSettingsScope) Done() bool {
	return scope == nil || scope.state.Load() != 0
}

// Commit closes the settings scope.
func (scope *SQLTransactionSettingsScope) Commit() error {
	return scope.finish()
}

// Rollback closes the settings scope.
func (scope *SQLTransactionSettingsScope) Rollback() error {
	return scope.finish()
}

// Close is an alias for Rollback for defer-friendly cleanup.
func (scope *SQLTransactionSettingsScope) Close() error {
	return scope.Rollback()
}

func (scope *SQLTransactionSettingsScope) finish() error {
	if scope == nil {
		return ErrSQLTransactionSettingsNil
	}
	if !scope.state.CompareAndSwap(0, 1) {
		return ErrSQLTransactionSettingsScopeClosed
	}
	if scope.cancel != nil {
		scope.cancel()
	}
	return nil
}
