package hatDataStructure

import (
	"context"
	"runtime"
)

// BeginMVCCTransaction starts a transaction with a repeatable read view.
// Values are retained by reference until the transaction is closed, while the
// snapshot maps and run lists are copied so later writes cannot change the
// view. Writes still use the T232 staged-commit semantics.
func (space *Space) BeginMVCCTransaction() (*SpaceTransaction, error) {
	return space.beginSpaceTransaction(true)
}

// Yield cooperatively gives the Go scheduler a chance to run other work. The
// MVCC view remains valid across the yield, and no Space or engine lock is
// held by the transaction between method calls. A cancelled context returns
// its cancellation error without changing the transaction.
func (tx *SpaceTransaction) Yield(ctx context.Context) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	runtime.Gosched()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type spaceTransactionSnapshot struct {
	engine SpaceEngine
	memtx  map[string][]byte
	vinyl  *lsmTableReadSnapshot
}

type lsmTableReadSnapshot struct {
	memtable map[string]lsmTableRecord
	runs     []*SealedUpsertRun
}

func (space *Space) captureSpaceTransactionSnapshot() *spaceTransactionSnapshot {
	snapshot := &spaceTransactionSnapshot{engine: space.engine}
	space.mu.RLock()
	defer space.mu.RUnlock()
	if space.engine == SpaceEngineMemtx {
		snapshot.memtx = make(map[string][]byte, len(space.memtx))
		for key, value := range space.memtx {
			snapshot.memtx[key] = value
		}
		return snapshot
	}
	table := space.vinyl
	table.mu.RLock()
	defer table.mu.RUnlock()
	memtable := make(map[string]lsmTableRecord, len(table.memtable))
	for key, record := range table.memtable {
		memtable[key] = record
	}
	snapshot.vinyl = &lsmTableReadSnapshot{
		memtable: memtable,
		runs:     append([]*SealedUpsertRun(nil), table.runs...),
	}
	return snapshot
}

func (snapshot *spaceTransactionSnapshot) Get(key string) ([]byte, bool) {
	if snapshot == nil || key == "" {
		return nil, false
	}
	if snapshot.engine == SpaceEngineMemtx {
		value, ok := snapshot.memtx[key]
		if !ok {
			return nil, false
		}
		return append([]byte(nil), value...), true
	}
	return snapshot.vinyl.Get(key)
}

func (snapshot *lsmTableReadSnapshot) Get(key string) ([]byte, bool) {
	if snapshot == nil || key == "" {
		return nil, false
	}
	if record, ok := snapshot.memtable[key]; ok {
		if record.deleted {
			return nil, false
		}
		return append([]byte(nil), record.value...), true
	}
	for _, run := range snapshot.runs {
		record, ok := run.Lookup(key)
		if !ok {
			continue
		}
		if record.Deleted {
			return nil, false
		}
		return append([]byte(nil), record.Value...), true
	}
	return nil, false
}
