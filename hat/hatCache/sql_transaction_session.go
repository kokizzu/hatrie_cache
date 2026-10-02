package hatCache

import (
	"errors"
	"sync"
)

// ErrNilSQLTransactionSession reports use of a nil session receiver.
var ErrNilSQLTransactionSession = errors.New("hatriecache: SQL transaction session is nil")

// SQLTransactionSession owns the defaults used by transactions created for one
// client session. Updating the defaults affects only transactions started
// after the update; existing transactions retain their captured options.
type SQLTransactionSession struct {
	mu      sync.RWMutex
	trie    *HatTrie
	options SQLTransactionOptions
}

// NewSQLTransactionSession creates a session with the backward-compatible
// snapshot, writable, and unlimited-time defaults.
func NewSQLTransactionSession(trie *HatTrie) (*SQLTransactionSession, error) {
	return NewSQLTransactionSessionWithOptions(trie, SQLTransactionOptions{})
}

// NewSQLTransactionSessionWithOptions creates a session after validating its
// defaults. The session does not own the trie and must not outlive its caller's
// trie lifecycle.
func NewSQLTransactionSessionWithOptions(trie *HatTrie, options SQLTransactionOptions) (*SQLTransactionSession, error) {
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	return &SQLTransactionSession{trie: trie, options: normalized}, nil
}

// Begin starts a transaction with the session's current defaults.
func (session *SQLTransactionSession) Begin() (*SQLTransaction, error) {
	if session == nil {
		return nil, ErrNilSQLTransactionSession
	}
	session.mu.RLock()
	trie := session.trie
	options := session.options
	session.mu.RUnlock()
	return BeginSQLTransactionWithOptions(trie, options)
}

// Options returns the defaults that will be used by the next transaction.
func (session *SQLTransactionSession) Options() SQLTransactionOptions {
	if session == nil {
		return SQLTransactionOptions{}
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.options
}

// SetOptions validates and replaces defaults for future transactions.
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
