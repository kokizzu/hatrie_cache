package hatCache

import (
	"errors"
	"sync"
)

// ErrNilSQLTransactionSession is returned when a nil session is asked to
// begin a transaction or change its defaults.
var ErrNilSQLTransactionSession = errors.New("SQL transaction session is nil")

// SQLTransactionSession stores validated per-client transaction defaults.
// The zero-value options preserve BeginSQLTransaction's snapshot, writable,
// and no-timeout behavior.
//
// A session is intentionally a small policy wrapper. It does not own or close
// the underlying HatTrie, and it does not add a durability policy that the
// transaction engine cannot enforce.
type SQLTransactionSession struct {
	mu      sync.RWMutex
	trie    *HatTrie
	options SQLTransactionOptions
}

// NewSQLTransactionSession creates a session bound to trie. The session does
// not take ownership of trie; callers retain responsibility for its lifetime.
func NewSQLTransactionSession(trie *HatTrie) (*SQLTransactionSession, error) {
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	return &SQLTransactionSession{trie: trie}, nil
}

// Options returns a consistent copy of the session defaults. A nil session
// reports the backward-compatible zero-value options.
func (session *SQLTransactionSession) Options() SQLTransactionOptions {
	if session == nil {
		return SQLTransactionOptions{}
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.options
}

// SetOptions validates and replaces the session defaults. Invalid options do
// not change the previously configured policy.
func (session *SQLTransactionSession) SetOptions(options SQLTransactionOptions) error {
	if session == nil {
		return ErrNilSQLTransactionSession
	}
	normalized, err := options.normalized()
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.options = normalized
	session.mu.Unlock()
	return nil
}

// ResetOptions restores the zero-value, backward-compatible transaction
// defaults. It is safe to call on a nil session.
func (session *SQLTransactionSession) ResetOptions() {
	if session == nil {
		return
	}
	session.mu.Lock()
	session.options = SQLTransactionOptions{}
	session.mu.Unlock()
}

// Begin opens a transaction using the defaults current when Begin starts. A
// concurrent SetOptions call affects only later Begin calls.
func (session *SQLTransactionSession) Begin() (*SQLTransaction, error) {
	if session == nil {
		return nil, ErrNilSQLTransactionSession
	}
	session.mu.RLock()
	trie, options := session.trie, session.options
	session.mu.RUnlock()
	return BeginSQLTransactionWithOptions(trie, options)
}
