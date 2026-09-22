package hatCache

import (
	"context"
	"errors"
	"runtime"
)

var errNilSQLTransactionYield = errors.New("SQL transaction is nil")

// Yield reaches a cooperative scheduling safe point without closing the
// transaction. SQLTransaction uses a private snapshot, so no live-cache lock
// is held while the goroutine yields. A canceled context is returned to the
// caller and the transaction remains usable; callers can choose to continue
// or call Rollback.
func (transaction *SQLTransaction) Yield(ctx context.Context) error {
	if transaction == nil {
		return errNilSQLTransactionYield
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	transaction.mu.Lock()
	if transaction.closed {
		err := transaction.closedError()
		transaction.mu.Unlock()
		return err
	}
	if err := transaction.checkTimeoutLocked(); err != nil {
		transaction.mu.Unlock()
		return err
	}
	transaction.mu.Unlock()

	runtime.Gosched()
	if err := ctx.Err(); err != nil {
		return err
	}

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return transaction.closedError()
	}
	return transaction.checkTimeoutLocked()
}
