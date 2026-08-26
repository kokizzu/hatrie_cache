package hatCache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// SQLTransaction is an optimistic snapshot-isolated transaction for scalar
// command SQL. Queries and staged writes run against a private snapshot; commit
// is rejected if any live mutation occurred after that snapshot.
type SQLTransaction struct {
	mu       sync.Mutex
	live     *HatTrie
	snapshot *HatTrie
	epoch    uint64
	staged   []CacheCommandRequest
	closed   bool
}

// BeginSQLTransaction captures a consistent private snapshot. A busy cache is
// retried when it changes during capture rather than exposing a mixed view.
func BeginSQLTransaction(trie *HatTrie) (*SQLTransaction, error) {
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	directory, err := os.MkdirTemp("", "hatrie-sql-transaction-*")
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "snapshot.bin.gz")
	var epoch uint64
	for attempt := 0; attempt < 3; attempt++ {
		before := atomic.LoadUint64(&trie.mutationEpoch)
		if err := trie.SaveSnapshotWithFormat(path, SnapshotFormatGzipBinary); err != nil {
			_ = os.RemoveAll(directory)
			return nil, err
		}
		after := atomic.LoadUint64(&trie.mutationEpoch)
		if before == after {
			epoch = after
			break
		}
	}
	if epoch == 0 && atomic.LoadUint64(&trie.mutationEpoch) != 0 {
		_ = os.RemoveAll(directory)
		return nil, fmt.Errorf("SQL transaction could not capture a stable snapshot")
	}
	snapshot := CreateHatTrie()
	if err := snapshot.LoadSnapshot(path); err != nil {
		snapshot.Destroy()
		_ = os.RemoveAll(directory)
		return nil, err
	}
	_ = os.RemoveAll(directory)
	return &SQLTransaction{live: trie, snapshot: snapshot, epoch: epoch}, nil
}

// Execute stages one or more scalar command-SQL mutations. SELECT and CALL
// reads are intentionally rejected; use Query for relational snapshot reads.
func (transaction *SQLTransaction) Execute(source string) (SQLMutationResult, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return SQLMutationResult{}, fmt.Errorf("SQL transaction is closed")
	}
	request, err := CompileSQL(source)
	if err != nil {
		return SQLMutationResult{}, err
	}
	payloads := []CacheCommandRequest{request}
	if normalizedCommand(request.Command) == "BATCH" {
		payloads, err = publicCommandBatchRequests(request)
		if err != nil {
			return SQLMutationResult{}, err
		}
	}
	for _, payload := range payloads {
		if !sqlTransactionWritableCommand(payload.Command) {
			return SQLMutationResult{}, fmt.Errorf("SQL transaction Execute supports scalar mutations only")
		}
	}
	response := transaction.snapshot.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Atomic: true, Batch: payloads})
	if !response.OK {
		return SQLMutationResult{Response: response}, fmt.Errorf("SQL transaction mutation failed: %s", response.Message)
	}
	transaction.staged = append(transaction.staged, payloads...)
	return SQLMutationResult{Affected: len(payloads), Response: response}, nil
}

// Query executes a relational read against the transaction snapshot, including
// staged writes that remain invisible to the live cache until Commit succeeds.
func (transaction *SQLTransaction) Query(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return SQLQueryResult{}, fmt.Errorf("SQL transaction is closed")
	}
	return ExecuteSQLQueryParameters(ctx, source, transaction.snapshot, parameters, options)
}

// Commit atomically publishes staged scalar writes only if the live cache still
// has the epoch from BeginSQLTransaction.
func (transaction *SQLTransaction) Commit() error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return fmt.Errorf("SQL transaction is closed")
	}
	response := transaction.live.executeSQLTransactionBatch(transaction.epoch, transaction.staged)
	transaction.closeLocked()
	if !response.OK {
		return fmt.Errorf("%s", response.Message)
	}
	return nil
}

// Rollback drops all private changes. It is safe to call repeatedly.
func (transaction *SQLTransaction) Rollback() error {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	transaction.closeLocked()
	return nil
}

func (transaction *SQLTransaction) closeLocked() {
	if transaction.closed {
		return
	}
	transaction.closed = true
	if transaction.snapshot != nil {
		transaction.snapshot.Destroy()
		transaction.snapshot = nil
	}
	transaction.staged = nil
}

func sqlTransactionWritableCommand(command string) bool {
	switch normalizedCommand(command) {
	case "SET", "SETSTR", "SETX", "SETSTRX", "SETINT", "SETINTX", "EXPIRE", "EXPIREAT", "DEL", "PERSIST":
		return true
	default:
		return false
	}
}
