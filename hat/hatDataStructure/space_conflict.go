package hatDataStructure

// BeginConflictDetectingTransaction starts an optimistic transaction that
// rejects a root commit when another write changed one of its staged keys
// after the transaction began. Regular transactions keep their existing
// last-writer-wins behavior.
func (space *Space) BeginConflictDetectingTransaction() (*SpaceTransaction, error) {
	if space == nil {
		return nil, ErrSpaceNil
	}
	space.conflictGate.Lock()
	defer space.conflictGate.Unlock()

	space.mu.Lock()
	if !space.conflictTracking {
		space.conflictTracking = true
		space.conflictVersions = make(map[string]uint64)
	}
	startGeneration := space.conflictGeneration
	space.mu.Unlock()

	return &SpaceTransaction{
		space:              space,
		conflictDetection:  true,
		conflictGeneration: startGeneration,
		changes:            make(map[string]spaceTransactionChange),
	}, nil
}

func (space *Space) recordSpaceMutationLocked(key string) {
	if !space.conflictTracking {
		return
	}
	space.conflictGeneration++
	space.conflictVersions[key] = space.conflictGeneration
}

func (space *Space) recordSpaceTransactionMutationsLocked(mutations []spaceTransactionMutation) {
	for _, mutation := range mutations {
		space.recordSpaceMutationLocked(mutation.key)
	}
}

func (space *Space) checkSpaceTransactionConflictsLocked(tx *SpaceTransaction) error {
	if tx == nil || !tx.conflictDetection {
		return nil
	}
	for key := range tx.changes {
		if space.conflictVersions[key] > tx.conflictGeneration {
			return ErrSpaceTransactionConflict
		}
	}
	return nil
}
