package hatCache

import "fmt"

// IntegrityReport reports validation of every live value through the canonical
// snapshot representation shared by backup, restore, and replication.
type IntegrityReport struct {
	Entries int
}

// IntegrityRepairResult reports the validation before and after an atomic
// in-memory compaction repair.
type IntegrityRepairResult struct {
	Before     IntegrityReport
	Compaction MemoryCompactionResult
	After      IntegrityReport
}

// CheckIntegrity captures every live typed value and validates its canonical
// snapshot encoding. This covers every value type that can be persisted.
func (ht *HatTrie) CheckIntegrity() (IntegrityReport, error) {
	if ht == nil {
		return IntegrityReport{}, ErrNilHatTrie
	}
	capture, err := ht.captureSnapshot()
	if err != nil {
		return IntegrityReport{}, fmt.Errorf("capture integrity snapshot: %w", err)
	}
	for _, page := range capture.pages {
		for _, entry := range page {
			if _, err := validateSnapshotEntry(entry); err != nil {
				return IntegrityReport{}, fmt.Errorf("validate integrity entry %q: %w", entry.Key, err)
			}
		}
	}
	return IntegrityReport{Entries: capture.count}, nil
}

// RepairIntegrity rebuilds the trie and typed backing storage atomically, then
// validates the repaired result. On a failed rebuild, live storage is retained.
func (ht *HatTrie) RepairIntegrity() (IntegrityRepairResult, error) {
	before, err := ht.CheckIntegrity()
	if err != nil {
		return IntegrityRepairResult{}, err
	}
	compaction, err := ht.CompactMemory()
	if err != nil {
		return IntegrityRepairResult{Before: before}, err
	}
	after, err := ht.CheckIntegrity()
	if err != nil {
		return IntegrityRepairResult{Before: before, Compaction: compaction}, err
	}
	return IntegrityRepairResult{Before: before, Compaction: compaction, After: after}, nil
}
