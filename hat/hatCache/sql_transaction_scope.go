package hatCache

import (
	"errors"
	"fmt"
	"sync"
)

// ErrSQLTransactionScopeClosed reports a scope that was already committed or
// rolled back.
var ErrSQLTransactionScopeClosed = errors.New("SQL transaction scope is closed")

// ErrNilSQLTransactionScopeCallback reports a missing callback passed to Scope.
var ErrNilSQLTransactionScopeCallback = errors.New("SQL transaction scope callback is required")

// SQLTransactionScope is a nested rollback boundary within an SQLTransaction.
// Committing the scope keeps its writes in the parent transaction; rolling it
// back discards only writes made after the scope began.
type SQLTransactionScope struct {
	mu          sync.Mutex
	transaction *SQLTransaction
	savepoint   string
	closed      bool
}

// BeginScope creates a nested rollback boundary. Scope names are generated and
// skip any caller-created savepoint names that happen to share the prefix.
func (transaction *SQLTransaction) BeginScope() (*SQLTransactionScope, error) {
	if transaction == nil {
		return nil, ErrSQLTransactionScopeClosed
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return nil, transaction.closedError()
	}
	if err := transaction.checkTimeoutLocked(); err != nil {
		return nil, err
	}
	var name string
	for {
		transaction.nextScopeID++
		name = fmt.Sprintf("__hatrie_scope_%d", transaction.nextScopeID)
		collision := false
		for _, savepoint := range transaction.savepoints {
			if savepoint.name == name {
				collision = true
				break
			}
		}
		if !collision {
			break
		}
	}
	if err := transaction.savepointLocked(name); err != nil {
		return nil, err
	}
	return &SQLTransactionScope{
		transaction: transaction,
		savepoint:   name,
	}, nil
}

// Scope runs callback inside a nested rollback boundary. A callback error is
// returned after rolling back only this scope, leaving the parent transaction
// open. A panic is rolled back and re-panicked so callers retain normal panic
// behavior without leaking staged scope writes.
func (transaction *SQLTransaction) Scope(callback func(*SQLTransaction) error) error {
	if callback == nil {
		return ErrNilSQLTransactionScopeCallback
	}
	scope, err := transaction.BeginScope()
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = scope.Rollback()
			panic(recovered)
		}
	}()
	if err := callback(transaction); err != nil {
		if rollbackErr := scope.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w (scope rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	return scope.Commit()
}

// Commit closes the scope and keeps its staged writes in the parent
// transaction.
func (scope *SQLTransactionScope) Commit() error {
	if scope == nil {
		return ErrSQLTransactionScopeClosed
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.closed {
		return ErrSQLTransactionScopeClosed
	}
	if err := scope.transaction.ReleaseSavepoint(scope.savepoint); err != nil {
		return err
	}
	scope.closed = true
	return nil
}

// Rollback closes the scope and discards writes made after it began.
func (scope *SQLTransactionScope) Rollback() error {
	if scope == nil {
		return ErrSQLTransactionScopeClosed
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.closed {
		return ErrSQLTransactionScopeClosed
	}
	if err := scope.transaction.RollbackTo(scope.savepoint); err != nil {
		return err
	}
	if err := scope.transaction.ReleaseSavepoint(scope.savepoint); err != nil {
		return err
	}
	scope.closed = true
	return nil
}
