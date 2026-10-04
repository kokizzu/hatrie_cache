package hatCache

import (
	"errors"
	"sync"
)

// ErrNilSQLTransactionSession is returned when a nil session is asked to
// publish transaction defaults or begin a transaction.
var ErrNilSQLTransactionSession = errors.New("SQL transaction session is nil")

// SQLTransactionSession stores one caller's transaction defaults. It is safe
// to update between transactions; Begin snapshots the current options so a
// transaction keeps one stable policy for its entire lifetime.
type SQLTransactionSession struct {
	mu      sync.RWMutex
	options SQLTransactionOptions
}

// NewSQLTransactionSession creates a session with validated transaction
// defaults. The zero-value options preserve existing behavior.
func NewSQLTransactionSession(options SQLTransactionOptions) (*SQLTransactionSession, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	return &SQLTransactionSession{options: options}, nil
}

// Options returns a copy of the current defaults.
func (session *SQLTransactionSession) Options() SQLTransactionOptions {
	if session == nil {
		return SQLTransactionOptions{}
	}
	session.mu.RLock()
	options := session.options
	session.mu.RUnlock()
	return options
}

// SetOptions validates and publishes new defaults for future transactions.
// Existing transactions retain the options captured by Begin.
func (session *SQLTransactionSession) SetOptions(options SQLTransactionOptions) error {
	if session == nil {
		return ErrNilSQLTransactionSession
	}
	options, err := options.normalized()
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.options = options
	session.mu.Unlock()
	return nil
}

// Reset restores the backward-compatible zero-value defaults.
func (session *SQLTransactionSession) Reset() {
	if session == nil {
		return
	}
	session.mu.Lock()
	session.options = SQLTransactionOptions{}
	session.mu.Unlock()
}

// Begin captures the session defaults and starts one SQL transaction.
func (session *SQLTransactionSession) Begin(trie *HatTrie) (*SQLTransaction, error) {
	if session == nil {
		return nil, ErrNilSQLTransactionSession
	}
	return BeginSQLTransactionWithOptions(trie, session.Options())
}
