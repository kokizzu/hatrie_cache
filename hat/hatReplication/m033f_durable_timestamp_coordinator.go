package hatReplication

import (
	"errors"
	"os"
	"sync"
)

// DurableGlobalTimestampOracle is an opt-in single-coordinator wrapper around
// GlobalTimestampOracle. Every state-changing operation is applied to a
// candidate state, atomically persisted, and only then published in memory.
// This prevents a successfully returned grant from being lost across a
// process restart. Consensus, leader election, and multi-process ownership
// remain caller responsibilities.
type DurableGlobalTimestampOracle struct {
	mu     sync.Mutex
	store  *GlobalTimestampOracleSnapshotFileStore
	oracle *GlobalTimestampOracle
}

// NewDurableGlobalTimestampOracle opens or creates a CRC-protected snapshot
// store. Term and initial are used only when path does not exist; an existing
// snapshot is restored after full validation.
func NewDurableGlobalTimestampOracle(path string, term uint64, initial int64) (*DurableGlobalTimestampOracle, error) {
	store, err := NewGlobalTimestampOracleSnapshotFileStore(path)
	if err != nil {
		return nil, err
	}
	snapshot, err := store.Load()
	if err == nil {
		oracle, restoreErr := NewGlobalTimestampOracleFromSnapshot(snapshot)
		if restoreErr != nil {
			return nil, restoreErr
		}
		return &DurableGlobalTimestampOracle{store: store, oracle: oracle}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	oracle, err := NewGlobalTimestampOracle(term, initial)
	if err != nil {
		return nil, err
	}
	if err := store.Save(oracle.Snapshot()); err != nil {
		return nil, err
	}
	return &DurableGlobalTimestampOracle{store: store, oracle: oracle}, nil
}

// Current returns the greatest durable timestamp published by the
// coordinator. A nil coordinator reports zero.
func (coordinator *DurableGlobalTimestampOracle) Current() int64 {
	if coordinator == nil {
		return 0
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.oracle.Current()
}

// Term returns the durable coordinator term. A nil coordinator reports zero.
func (coordinator *DurableGlobalTimestampOracle) Term() uint64 {
	if coordinator == nil {
		return 0
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.oracle.Term()
}

// Snapshot returns a deterministic copy of the currently published state. A
// nil coordinator returns an empty snapshot.
func (coordinator *DurableGlobalTimestampOracle) Snapshot() GlobalTimestampOracleSnapshot {
	if coordinator == nil {
		return GlobalTimestampOracleSnapshot{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.oracle.Snapshot()
}

// AdvanceTerm durably fences older coordinator terms before publishing the
// new term. Equal terms are a no-op.
func (coordinator *DurableGlobalTimestampOracle) AdvanceTerm(term uint64) error {
	if coordinator == nil {
		return ErrGlobalTimestampOracleInvalid
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	candidate, err := NewGlobalTimestampOracleFromSnapshot(coordinator.oracle.Snapshot())
	if err != nil {
		return err
	}
	if err := candidate.AdvanceTerm(term); err != nil {
		return err
	}
	return coordinator.publishLocked(candidate)
}

// Reserve durably reserves one contiguous range. When persistence fails, the
// candidate grant is discarded and no in-memory timestamp is advanced.
func (coordinator *DurableGlobalTimestampOracle) Reserve(request GlobalTimestampRequest) (GlobalTimestampGrant, error) {
	if coordinator == nil {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleInvalid
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	candidate, err := NewGlobalTimestampOracleFromSnapshot(coordinator.oracle.Snapshot())
	if err != nil {
		return GlobalTimestampGrant{}, err
	}
	grant, err := candidate.Reserve(request)
	if err != nil {
		return GlobalTimestampGrant{}, err
	}
	if err := coordinator.publishLocked(candidate); err != nil {
		return GlobalTimestampGrant{}, err
	}
	return grant, nil
}

func (coordinator *DurableGlobalTimestampOracle) publishLocked(candidate *GlobalTimestampOracle) error {
	if candidate == nil {
		return ErrGlobalTimestampOracleInvalid
	}
	previous := coordinator.oracle.Snapshot()
	next := candidate.Snapshot()
	if globalTimestampOracleSnapshotsEqual(previous, next) {
		return nil
	}
	if err := coordinator.store.Save(next); err != nil {
		return err
	}
	coordinator.oracle = candidate
	return nil
}

func globalTimestampOracleSnapshotsEqual(left, right GlobalTimestampOracleSnapshot) bool {
	if left.Term != right.Term || left.Current != right.Current || len(left.Nodes) != len(right.Nodes) {
		return false
	}
	for index := range left.Nodes {
		if left.Nodes[index] != right.Nodes[index] {
			return false
		}
	}
	return true
}
