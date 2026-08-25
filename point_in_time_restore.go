package hatriecache

import (
	"errors"
	"fmt"
	"strings"
)

// PointInTimeRestoreOptions identifies one snapshot and journal target. A
// TargetSequence of zero selects the current journal tail.
type PointInTimeRestoreOptions struct {
	SnapshotPath   string
	JournalPath    string
	TargetSequence uint64
}

// PointInTimeRestoreReport describes a fully loaded and replay-verified
// in-memory recovery. The returned trie is owned by the caller.
type PointInTimeRestoreReport struct {
	SnapshotPath     string `json:"snapshot_path"`
	JournalPath      string `json:"journal_path"`
	SnapshotSequence uint64 `json:"snapshot_sequence"`
	AppliedThrough   uint64 `json:"applied_through"`
	RecoveredKeys    int    `json:"recovered_keys"`
	Verified         bool   `json:"verified"`
}

// RestorePointInTime loads a validated snapshot, replays no journal entry
// after TargetSequence, and returns a semantically verified recovered trie.
func RestorePointInTime(options PointInTimeRestoreOptions) (*HatTrie, PointInTimeRestoreReport, error) {
	options.SnapshotPath = strings.TrimSpace(options.SnapshotPath)
	options.JournalPath = strings.TrimSpace(options.JournalPath)
	if options.SnapshotPath == "" {
		return nil, PointInTimeRestoreReport{}, errors.New("hatriecache: point-in-time snapshot path is required")
	}
	if options.JournalPath == "" {
		return nil, PointInTimeRestoreReport{}, errors.New("hatriecache: point-in-time journal path is required")
	}
	restored := CreateHatTrie()
	metadata, err := restored.LoadSnapshotWithMetadata(options.SnapshotPath)
	if err != nil {
		restored.Destroy()
		return nil, PointInTimeRestoreReport{}, err
	}
	journal, err := OpenCommandJournal(options.JournalPath)
	if err != nil {
		restored.Destroy()
		return nil, PointInTimeRestoreReport{}, err
	}
	defer journal.Close()
	target := options.TargetSequence
	if target == 0 {
		target = journal.Sequence()
	}
	if target < metadata.JournalSequence {
		restored.Destroy()
		return nil, PointInTimeRestoreReport{}, fmt.Errorf("hatriecache: requested journal sequence %d precedes snapshot sequence %d", target, metadata.JournalSequence)
	}
	applied, err := journal.ReplayThrough(restored, metadata.JournalSequence, target)
	if err != nil {
		restored.Destroy()
		return nil, PointInTimeRestoreReport{}, err
	}
	return restored, PointInTimeRestoreReport{
		SnapshotPath:     options.SnapshotPath,
		JournalPath:      options.JournalPath,
		SnapshotSequence: metadata.JournalSequence,
		AppliedThrough:   applied,
		RecoveredKeys:    restored.Size(),
		Verified:         true,
	}, nil
}
