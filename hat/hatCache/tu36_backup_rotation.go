package hatCache

import (
	"context"
	"errors"
	"os"
	"time"
)

var (
	ErrBackupRotationInvalidPolicy    = errors.New("hatriecache: invalid backup rotation policy")
	ErrBackupRotationInvalidState     = errors.New("hatriecache: invalid backup rotation state")
	ErrBackupRotationJournalRegressed = errors.New("hatriecache: backup rotation journal sequence regressed")
	ErrBackupRotationClockRegressed   = errors.New("hatriecache: backup rotation clock regressed")
)

// BackupRotationReason explains why a rotation decision was made.
type BackupRotationReason string

const (
	BackupRotationReasonInitial         BackupRotationReason = "initial"
	BackupRotationReasonJournalDelta    BackupRotationReason = "journal-delta"
	BackupRotationReasonMaximumInterval BackupRotationReason = "maximum-interval"
	BackupRotationReasonNotDue          BackupRotationReason = "not-due"
)

// BackupRotationPolicy controls opt-in incremental backup cadence and
// repository retention. A zero value has no trigger and is invalid when used.
// MinimumInterval gates journal-delta-triggered backups. MaximumInterval is a
// hard upper bound and can trigger even while MinimumInterval is active.
type BackupRotationPolicy struct {
	MinimumInterval      time.Duration
	MaximumInterval      time.Duration
	JournalSequenceDelta uint64
	Retain               int
	RetainBytes          int64
}

// BackupRotationState is the immutable state needed for one decision.
type BackupRotationState struct {
	HasBackup              bool
	LastCreatedAt          time.Time
	LastJournalSequence    uint64
	CurrentJournalSequence uint64
}

// BackupRotationDecision reports whether a backup should be created.
type BackupRotationDecision struct {
	Due    bool
	Reason BackupRotationReason
}

// BackupRotationResult reports an optional backup created by the helper.
type BackupRotationResult struct {
	Manifest BackupBundleManifest
	Decision BackupRotationDecision
	Created  bool
}

// Validate checks the policy without touching the filesystem.
func (policy BackupRotationPolicy) Validate() error {
	if policy.MinimumInterval < 0 || policy.MaximumInterval < 0 {
		return ErrBackupRotationInvalidPolicy
	}
	if policy.MaximumInterval > 0 && policy.MinimumInterval > policy.MaximumInterval {
		return ErrBackupRotationInvalidPolicy
	}
	if policy.MaximumInterval == 0 && policy.JournalSequenceDelta == 0 {
		return ErrBackupRotationInvalidPolicy
	}
	if policy.Retain < 0 || policy.RetainBytes < 0 {
		return ErrBackupRotationInvalidPolicy
	}
	return nil
}

// Decide determines whether the next incremental backup is due.
func (policy BackupRotationPolicy) Decide(now time.Time, state BackupRotationState) (BackupRotationDecision, error) {
	if err := policy.Validate(); err != nil {
		return BackupRotationDecision{}, err
	}
	if now.IsZero() {
		return BackupRotationDecision{}, ErrBackupRotationInvalidState
	}
	if !state.HasBackup {
		return BackupRotationDecision{Due: true, Reason: BackupRotationReasonInitial}, nil
	}
	if state.CurrentJournalSequence < state.LastJournalSequence {
		return BackupRotationDecision{}, ErrBackupRotationJournalRegressed
	}
	if state.LastCreatedAt.IsZero() {
		return BackupRotationDecision{}, ErrBackupRotationInvalidState
	}
	if now.Before(state.LastCreatedAt) {
		return BackupRotationDecision{}, ErrBackupRotationClockRegressed
	}

	elapsed := now.Sub(state.LastCreatedAt)
	if policy.MaximumInterval > 0 && elapsed >= policy.MaximumInterval {
		return BackupRotationDecision{Due: true, Reason: BackupRotationReasonMaximumInterval}, nil
	}
	if policy.JournalSequenceDelta > 0 &&
		state.CurrentJournalSequence-state.LastJournalSequence >= policy.JournalSequenceDelta &&
		(policy.MinimumInterval == 0 || elapsed >= policy.MinimumInterval) {
		return BackupRotationDecision{Due: true, Reason: BackupRotationReasonJournalDelta}, nil
	}
	return BackupRotationDecision{Reason: BackupRotationReasonNotDue}, nil
}

// CreateIncrementalBackupRepositoryIfDue creates an incremental backup only
// when policy says it is due. The first backup is always created. This helper
// is intended for one scheduler per repository; the underlying repository
// writer remains the serialization boundary for concurrent backup calls.
func CreateIncrementalBackupRepositoryIfDue(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions, policy BackupRotationPolicy) (BackupRotationResult, error) {
	return CreateIncrementalBackupRepositoryIfDueWithContext(context.Background(), path, trie, journal, options, policy)
}

// CreateIncrementalBackupRepositoryIfDueWithContext is the context-aware form
// of CreateIncrementalBackupRepositoryIfDue.
func CreateIncrementalBackupRepositoryIfDueWithContext(ctx context.Context, path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions, policy BackupRotationPolicy) (BackupRotationResult, error) {
	if err := policy.Validate(); err != nil {
		return BackupRotationResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = normalizeBackupContext(ctx)
	if err := checkBackupContext(ctx); err != nil {
		return BackupRotationResult{}, err
	}
	latest, err := readBackupRepositoryManifest(path, "")
	hasBackup := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return BackupRotationResult{}, err
	}
	now := options.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	currentSequence := latest.JournalSequence
	if journal != nil {
		journal.mu.Lock()
		currentSequence = journal.lastSequenceLocked()
		journal.mu.Unlock()
	}
	decision, err := policy.Decide(now, BackupRotationState{
		HasBackup:              hasBackup,
		LastCreatedAt:          latest.CreatedAt,
		LastJournalSequence:    latest.JournalSequence,
		CurrentJournalSequence: currentSequence,
	})
	if err != nil {
		return BackupRotationResult{}, err
	}
	if !decision.Due {
		return BackupRotationResult{Manifest: latest, Decision: decision}, nil
	}
	if policy.Retain > 0 && options.RepositoryRetain == 0 {
		options.RepositoryRetain = policy.Retain
	}
	if policy.RetainBytes > 0 && options.RepositoryRetainBytes == 0 {
		options.RepositoryRetainBytes = policy.RetainBytes
	}
	options.CreatedAt = now
	manifest, err := CreateIncrementalBackupRepositoryWithContext(ctx, path, trie, journal, options)
	if err != nil {
		return BackupRotationResult{}, err
	}
	return BackupRotationResult{Manifest: manifest, Decision: decision, Created: true}, nil
}
