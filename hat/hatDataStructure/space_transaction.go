package hatDataStructure

import (
	"errors"
	"sort"
)

var (
	ErrSpaceTransactionNil       = errors.New("hatDataStructure: space transaction is nil")
	ErrSpaceTransactionClosed    = errors.New("hatDataStructure: space transaction is closed")
	ErrSpaceTransactionChildOpen = errors.New("hatDataStructure: nested space transaction is still open")
	ErrSpaceTransactionConflict  = errors.New("hatDataStructure: space transaction conflict")
)

// SpaceTransaction stages key/value mutations for one Space. A transaction's
// staged values are private until Commit succeeds. Transactions are not safe
// for concurrent use; the owning Space remains safe for concurrent callers.
type SpaceTransaction struct {
	space              *Space
	parent             *SpaceTransaction
	snapshot           *spaceTransactionSnapshot
	changes            map[string]spaceTransactionChange
	conflictDetection  bool
	conflictGeneration uint64
	children           int
	closed             bool
}

type spaceTransactionChange struct {
	value   []byte
	deleted bool
}

type spaceTransactionMutation struct {
	key          string
	oldValue     []byte
	oldExists    bool
	newValue     []byte
	deleted      bool
	auditID      uint64
	onReplace    SpaceReplace
	afterReplace SpaceReplaceAudit
}

type lsmTableTransactionSnapshot struct {
	memtable              map[string]lsmTableRecord
	runs                  []*SealedUpsertRun
	memtableBytes         int
	memtableTombstones    int
	compactionCount       uint64
	compactionInputBytes  uint64
	compactionOutputBytes uint64
}

// BeginTransaction starts a transaction with an empty private write set.
// Reads see the transaction's staged values, then the current Space state.
// Concurrent writes are allowed and use the current state when Commit runs;
// conflict detection is intentionally a later transaction feature.
func (space *Space) BeginTransaction() (*SpaceTransaction, error) {
	if space == nil {
		return nil, ErrSpaceNil
	}
	return &SpaceTransaction{
		space:   space,
		changes: make(map[string]spaceTransactionChange),
	}, nil
}

func (space *Space) beginSpaceTransaction(mvcc bool) (*SpaceTransaction, error) {
	if space == nil {
		return nil, ErrSpaceNil
	}
	tx := &SpaceTransaction{
		space:   space,
		changes: make(map[string]spaceTransactionChange),
	}
	if mvcc {
		tx.snapshot = space.captureSpaceTransactionSnapshot()
	}
	return tx, nil
}

// BeginNested starts a child transaction. A child Commit merges its final
// changes into the parent without touching storage; a child Rollback discards
// only the child changes.
func (tx *SpaceTransaction) BeginNested() (*SpaceTransaction, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	tx.children++
	return &SpaceTransaction{
		space:              tx.space,
		parent:             tx,
		snapshot:           tx.snapshot,
		changes:            make(map[string]spaceTransactionChange),
		conflictDetection:  tx.conflictDetection,
		conflictGeneration: tx.conflictGeneration,
	}, nil
}

// Put stages an insert or replacement and copies value immediately.
func (tx *SpaceTransaction) Put(key string, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if key == "" {
		return ErrSpaceKeyRequired
	}
	if err := tx.space.validateTransactionValue(value); err != nil {
		return err
	}
	tx.changes[key] = spaceTransactionChange{value: append([]byte(nil), value...)}
	return nil
}

// Get reads the transaction view and returns an independent value copy.
func (tx *SpaceTransaction) Get(key string) ([]byte, bool) {
	if tx == nil || !tx.isOpen() || key == "" {
		return nil, false
	}
	if change, ok := tx.changes[key]; ok {
		if change.deleted {
			return nil, false
		}
		return append([]byte(nil), change.value...), true
	}
	if tx.parent != nil {
		return tx.parent.Get(key)
	}
	if tx.snapshot != nil {
		return tx.snapshot.Get(key)
	}
	return tx.space.Get(key)
}

// Delete stages deletion of a visible key. Deleting a missing key is a no-op.
func (tx *SpaceTransaction) Delete(key string) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if key == "" {
		return ErrSpaceKeyRequired
	}
	if _, ok := tx.Get(key); !ok {
		return nil
	}
	tx.changes[key] = spaceTransactionChange{deleted: true}
	return nil
}

// Commit applies the transaction. A nested commit merges into its parent; the
// root commit validates all final mutations before changing storage.
func (tx *SpaceTransaction) Commit() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.children > 0 {
		return ErrSpaceTransactionChildOpen
	}
	if tx.parent != nil {
		if !tx.parent.isOpen() {
			return ErrSpaceTransactionClosed
		}
		for key, change := range tx.changes {
			tx.parent.changes[key] = cloneSpaceTransactionChange(change)
		}
		tx.closed = true
		tx.changes = nil
		tx.parent.children--
		return nil
	}
	if err := tx.space.commitSpaceTransaction(tx); err != nil {
		return err
	}
	tx.closed = true
	tx.changes = nil
	return nil
}

// Rollback discards the transaction. A parent cannot be rolled back while a
// child is still open, which prevents an orphaned child from being committed.
func (tx *SpaceTransaction) Rollback() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.children > 0 {
		return ErrSpaceTransactionChildOpen
	}
	tx.closed = true
	tx.changes = nil
	if tx.parent != nil {
		tx.parent.children--
	}
	return nil
}

func (tx *SpaceTransaction) ensureOpen() error {
	if tx == nil {
		return ErrSpaceTransactionNil
	}
	if tx.space == nil {
		return ErrSpaceNil
	}
	if !tx.isOpen() {
		return ErrSpaceTransactionClosed
	}
	return nil
}

func (tx *SpaceTransaction) isOpen() bool {
	if tx == nil || tx.space == nil || tx.closed {
		return false
	}
	for parent := tx.parent; parent != nil; parent = parent.parent {
		if parent.closed {
			return false
		}
	}
	return true
}

func cloneSpaceTransactionChange(change spaceTransactionChange) spaceTransactionChange {
	return spaceTransactionChange{
		value:   append([]byte(nil), change.value...),
		deleted: change.deleted,
	}
}

func (space *Space) validateTransactionValue(value []byte) error {
	if space.engine == SpaceEngineVinyl {
		if len(value) > space.vinyl.config.runConfig.maxValueBytes {
			return ErrLSMTableValueTooLarge
		}
		return nil
	}
	if len(value) > space.memtxOptions.MaxValueBytes {
		return ErrSpaceValueTooLarge
	}
	return nil
}

func (space *Space) commitSpaceTransaction(tx *SpaceTransaction) error {
	space.mu.Lock()
	defer space.mu.Unlock()
	if err := space.checkSpaceTransactionConflictsLocked(tx); err != nil {
		return err
	}
	changes := tx.changes

	if space.engine == SpaceEngineVinyl {
		table := space.vinyl
		table.mu.Lock()
		defer table.mu.Unlock()
		mutations, err := space.prepareSpaceTransactionMutationsLocked(changes)
		if err != nil {
			return err
		}
		snapshot := snapshotLSMTableLocked(table)
		if err := space.applySpaceTransactionMutationsLocked(mutations); err != nil {
			restoreLSMTableLocked(table, snapshot)
			return err
		}
		space.recordSpaceTransactionMutationsLocked(mutations)
		space.emitSpaceTransactionCallbacks(mutations)
		return nil
	}

	mutations, err := space.prepareSpaceTransactionMutationsLocked(changes)
	if err != nil {
		return err
	}
	if err := space.applySpaceTransactionMutationsLocked(mutations); err != nil {
		return err
	}
	space.recordSpaceTransactionMutationsLocked(mutations)
	space.emitSpaceTransactionCallbacks(mutations)
	return nil
}

func (space *Space) prepareSpaceTransactionMutationsLocked(changes map[string]spaceTransactionChange) ([]spaceTransactionMutation, error) {
	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	mutations := make([]spaceTransactionMutation, 0, len(keys))
	recordCount := 0
	if space.engine == SpaceEngineMemtx {
		recordCount = len(space.memtx)
	}
	for _, key := range keys {
		change := changes[key]
		oldValue, oldExists := space.transactionValueLocked(key)
		if change.deleted {
			if !oldExists {
				continue
			}
			recordCount--
		} else {
			if len(change.value) > space.transactionMaxValueBytes() {
				if space.engine == SpaceEngineVinyl {
					return nil, ErrLSMTableValueTooLarge
				}
				return nil, ErrSpaceValueTooLarge
			}
			if !oldExists {
				recordCount++
			}
		}
		mutations = append(mutations, spaceTransactionMutation{
			key:       key,
			oldValue:  oldValue,
			oldExists: oldExists,
			newValue:  append([]byte(nil), change.value...),
			deleted:   change.deleted,
		})
	}
	if space.engine == SpaceEngineMemtx && recordCount > space.memtxOptions.MaxRecords {
		return nil, ErrSpaceFull
	}
	for index := range mutations {
		mutation := &mutations[index]
		if err := space.runBeforeReplaceLocked(mutation.key, mutation.oldValue, mutation.newValue, mutation.oldExists, mutation.deleted); err != nil {
			return nil, err
		}
	}
	if space.afterReplace != nil {
		if uint64(len(mutations)) > ^uint64(0)-space.nextTransactionID {
			return nil, ErrSpaceTransactionIDExhausted
		}
		for index := range mutations {
			transactionID, err := space.reserveTransactionIDLocked()
			if err != nil {
				return nil, err
			}
			mutations[index].auditID = transactionID
		}
	}
	for index := range mutations {
		mutation := &mutations[index]
		if space.onReplace != nil {
			mutation.onReplace = cloneSpaceReplace(mutation.key, mutation.oldValue, mutation.newValue, mutation.oldExists, mutation.deleted)
		}
		if space.afterReplace != nil {
			mutation.afterReplace = cloneSpaceReplaceAudit(mutation.auditID, mutation.key, mutation.oldValue, mutation.newValue, mutation.oldExists, mutation.deleted)
		}
	}
	return mutations, nil
}

func (space *Space) transactionValueLocked(key string) ([]byte, bool) {
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.getLocked(key)
	}
	value, ok := space.memtx[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), value...), true
}

func (space *Space) transactionMaxValueBytes() int {
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.config.runConfig.maxValueBytes
	}
	return space.memtxOptions.MaxValueBytes
}

func (space *Space) applySpaceTransactionMutationsLocked(mutations []spaceTransactionMutation) error {
	if space.engine == SpaceEngineVinyl {
		for _, mutation := range mutations {
			var err error
			if mutation.deleted {
				err = space.vinyl.deleteLocked(mutation.key)
			} else {
				err = space.vinyl.putLocked(mutation.key, mutation.newValue)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, mutation := range mutations {
		if mutation.deleted {
			delete(space.memtx, mutation.key)
			continue
		}
		space.memtx[mutation.key] = append([]byte(nil), mutation.newValue...)
	}
	return nil
}

func (space *Space) emitSpaceTransactionCallbacks(mutations []spaceTransactionMutation) {
	for _, mutation := range mutations {
		if space.onReplace != nil {
			space.onReplace(mutation.onReplace)
		}
		if space.afterReplace != nil {
			space.afterReplace(mutation.afterReplace)
		}
	}
}

func snapshotLSMTableLocked(table *LSMTable) lsmTableTransactionSnapshot {
	var memtable map[string]lsmTableRecord
	if table.memtable != nil {
		memtable = make(map[string]lsmTableRecord, len(table.memtable))
		for key, record := range table.memtable {
			memtable[key] = record
		}
	}
	return lsmTableTransactionSnapshot{
		memtable:              memtable,
		runs:                  append([]*SealedUpsertRun(nil), table.runs...),
		memtableBytes:         table.memtableBytes,
		memtableTombstones:    table.memtableTombstones,
		compactionCount:       table.compactionCount,
		compactionInputBytes:  table.compactionInputBytes,
		compactionOutputBytes: table.compactionOutputBytes,
	}
}

func restoreLSMTableLocked(table *LSMTable, snapshot lsmTableTransactionSnapshot) {
	table.memtable = snapshot.memtable
	table.runs = snapshot.runs
	table.memtableBytes = snapshot.memtableBytes
	table.memtableTombstones = snapshot.memtableTombstones
	table.compactionCount = snapshot.compactionCount
	table.compactionInputBytes = snapshot.compactionInputBytes
	table.compactionOutputBytes = snapshot.compactionOutputBytes
}
