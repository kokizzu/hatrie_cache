package hatCache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// SQLTransaction is an optimistic snapshot-isolated transaction for scalar
// command SQL. Queries and staged writes run against a private snapshot; commit
// is rejected if any live mutation occurred after that snapshot.
type SQLTransaction struct {
	mu                   sync.Mutex
	live                 *HatTrie
	snapshot             *HatTrie
	epoch                uint64
	isolation            SQLTransactionIsolation
	serializableLockHeld bool
	staged               []CacheCommandRequest
	savepoints           []sqlTransactionSavepoint
	closed               bool
}

type sqlTransactionSavepoint struct {
	name     string
	staged   int
	snapshot *HatTrie
}

// BeginSQLTransaction captures a consistent private snapshot. A busy cache is
// retried when it changes during capture rather than exposing a mixed view.
func BeginSQLTransaction(trie *HatTrie) (*SQLTransaction, error) {
	return BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{})
}

// BeginSQLTransactionWithOptions captures a consistent private snapshot with
// the requested isolation policy. Snapshot is the backward-compatible default;
// serializable additionally holds the command transaction lock until the
// transaction closes. Direct typed mutations still cause the existing epoch
// conflict check at commit.
func BeginSQLTransactionWithOptions(trie *HatTrie, options SQLTransactionOptions) (transaction *SQLTransaction, err error) {
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	options, err = options.normalized()
	if err != nil {
		return nil, err
	}
	serializableLockHeld := options.Isolation == SQLTransactionIsolationSerializable
	if serializableLockHeld {
		trie.commandTransactionMu.Lock()
		defer func() {
			if transaction == nil {
				trie.commandTransactionMu.Unlock()
			}
		}()
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
	return &SQLTransaction{
		live:                 trie,
		snapshot:             snapshot,
		epoch:                epoch,
		isolation:            options.Isolation,
		serializableLockHeld: serializableLockHeld,
	}, nil
}

// Isolation reports the policy selected when the transaction began. A nil
// transaction reports the backward-compatible snapshot default.
func (transaction *SQLTransaction) Isolation() SQLTransactionIsolation {
	if transaction == nil {
		return DefaultSQLTransactionIsolation
	}
	return transaction.isolation
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

// Savepoint records the current staged-write boundary and a private snapshot.
// RollbackTo restores that snapshot without discarding earlier transaction work.
func (transaction *SQLTransaction) Savepoint(name string) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return fmt.Errorf("SQL transaction is closed")
	}
	name, err := normalizeSQLSavepointName(name)
	if err != nil {
		return err
	}
	for _, savepoint := range transaction.savepoints {
		if savepoint.name == name {
			return fmt.Errorf("SQL savepoint %q already exists", name)
		}
	}
	snapshot, err := cloneSQLTransactionSnapshot(transaction.snapshot)
	if err != nil {
		return err
	}
	transaction.savepoints = append(transaction.savepoints, sqlTransactionSavepoint{name: name, staged: len(transaction.staged), snapshot: snapshot})
	return nil
}

// RollbackTo restores a previously named savepoint and discards later writes
// and savepoints while preserving the named savepoint for repeated rollback.
func (transaction *SQLTransaction) RollbackTo(name string) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return fmt.Errorf("SQL transaction is closed")
	}
	name, err := normalizeSQLSavepointName(name)
	if err != nil {
		return err
	}
	index := -1
	for position := len(transaction.savepoints) - 1; position >= 0; position-- {
		if transaction.savepoints[position].name == name {
			index = position
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("SQL savepoint %q does not exist", name)
	}
	restored, err := cloneSQLTransactionSnapshot(transaction.savepoints[index].snapshot)
	if err != nil {
		return err
	}
	transaction.snapshot.Destroy()
	transaction.snapshot = restored
	transaction.staged = transaction.staged[:transaction.savepoints[index].staged]
	for position := index + 1; position < len(transaction.savepoints); position++ {
		transaction.savepoints[position].snapshot.Destroy()
	}
	transaction.savepoints = transaction.savepoints[:index+1]
	return nil
}

// ReleaseSavepoint deletes a named savepoint without changing staged writes.
func (transaction *SQLTransaction) ReleaseSavepoint(name string) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed {
		return fmt.Errorf("SQL transaction is closed")
	}
	name, err := normalizeSQLSavepointName(name)
	if err != nil {
		return err
	}
	for position := len(transaction.savepoints) - 1; position >= 0; position-- {
		if transaction.savepoints[position].name != name {
			continue
		}
		transaction.savepoints[position].snapshot.Destroy()
		copy(transaction.savepoints[position:], transaction.savepoints[position+1:])
		transaction.savepoints[len(transaction.savepoints)-1] = sqlTransactionSavepoint{}
		transaction.savepoints = transaction.savepoints[:len(transaction.savepoints)-1]
		return nil
	}
	return fmt.Errorf("SQL savepoint %q does not exist", name)
}

func normalizeSQLSavepointName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("SQL savepoint name is required")
	}
	for index := 0; index < len(name); index++ {
		value := name[index]
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '_' {
			continue
		}
		return "", fmt.Errorf("invalid SQL savepoint name %q", name)
	}
	return strings.ToLower(name), nil
}

func cloneSQLTransactionSnapshot(source *HatTrie) (*HatTrie, error) {
	directory, err := os.MkdirTemp("", "hatrie-sql-savepoint-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "snapshot.bin.gz")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatGzipBinary); err != nil {
		return nil, err
	}
	clone := CreateHatTrie()
	if err := clone.LoadSnapshot(path); err != nil {
		clone.Destroy()
		return nil, err
	}
	return clone, nil
}

func (transaction *SQLTransaction) closeLocked() {
	if transaction.closed {
		return
	}
	transaction.closed = true
	if transaction.serializableLockHeld && transaction.live != nil {
		transaction.live.commandTransactionMu.Unlock()
		transaction.serializableLockHeld = false
	}
	if transaction.snapshot != nil {
		transaction.snapshot.Destroy()
		transaction.snapshot = nil
	}
	transaction.staged = nil
	for index := range transaction.savepoints {
		if transaction.savepoints[index].snapshot != nil {
			transaction.savepoints[index].snapshot.Destroy()
		}
	}
	transaction.savepoints = nil
}

func sqlTransactionWritableCommand(command string) bool {
	switch normalizedCommand(command) {
	case "SET", "SETSTR", "SETX", "SETSTRX", "SETINT", "SETINTX", "EXPIRE", "EXPIREAT", "DEL", "PERSIST":
		return true
	default:
		return false
	}
}
