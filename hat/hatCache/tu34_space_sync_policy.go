package hatCache

import (
	"errors"
	"strings"
	"time"
)

// CommandJournalSpaceSyncMode selects the durability behavior for one named
// command-journal space.
type CommandJournalSpaceSyncMode string

const (
	// CommandJournalSpaceSyncModeSynchronous preserves the normal journal
	// contract: the WAL is synced before the mutation is applied and returned.
	CommandJournalSpaceSyncModeSynchronous CommandJournalSpaceSyncMode = "synchronous"
	// CommandJournalSpaceSyncModePeriodic appends and applies immediately, then
	// flushes pending bytes on the configured interval or an explicit Flush.
	CommandJournalSpaceSyncModePeriodic CommandJournalSpaceSyncMode = "periodic"
	// CommandJournalSpaceSyncModeDisabled applies mutations without appending
	// them to this command journal. Backups or another durable layer must cover
	// this space if its contents need recovery.
	CommandJournalSpaceSyncModeDisabled CommandJournalSpaceSyncMode = "disabled"
)

var (
	ErrCommandJournalSpaceNameRequired = errors.New("hatriecache: command journal space name is required")
	ErrCommandJournalSpaceSyncMode     = errors.New("hatriecache: command journal space sync mode is invalid")
	ErrCommandJournalSpaceSyncInterval = errors.New("hatriecache: command journal space sync interval is invalid")
)

// CommandJournalSpaceSyncPolicy configures one named journal space. An empty
// mode is normalized to synchronous to preserve the existing durability
// default. Periodic mode requires a positive interval.
type CommandJournalSpaceSyncPolicy struct {
	Mode     CommandJournalSpaceSyncMode
	Interval time.Duration
}

// CommandJournalSpace is an explicit named view over a command journal. The
// space name is never inferred from a cache key, avoiding accidental changes
// to existing command semantics.
type CommandJournalSpace struct {
	journal *CommandJournal
	name    string
}

func normalizeCommandJournalSpaceSyncPolicy(policy CommandJournalSpaceSyncPolicy) (CommandJournalSpaceSyncPolicy, error) {
	policy.Mode = CommandJournalSpaceSyncMode(strings.ToLower(strings.TrimSpace(string(policy.Mode))))
	if policy.Mode == "" {
		policy.Mode = CommandJournalSpaceSyncModeSynchronous
	}
	switch policy.Mode {
	case CommandJournalSpaceSyncModeSynchronous, CommandJournalSpaceSyncModeDisabled:
		if policy.Interval != 0 {
			return CommandJournalSpaceSyncPolicy{}, ErrCommandJournalSpaceSyncInterval
		}
	case CommandJournalSpaceSyncModePeriodic:
		if policy.Interval <= 0 {
			return CommandJournalSpaceSyncPolicy{}, ErrCommandJournalSpaceSyncInterval
		}
	default:
		return CommandJournalSpaceSyncPolicy{}, ErrCommandJournalSpaceSyncMode
	}
	return policy, nil
}

// OpenSpace registers a named journal space and returns its explicit command
// handle. Registration is opt-in; ordinary CommandJournal callers retain the
// existing synchronous/group-commit behavior.
func (journal *CommandJournal) OpenSpace(name string, policy CommandJournalSpaceSyncPolicy) (*CommandJournalSpace, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrCommandJournalSpaceNameRequired
	}
	normalized, err := normalizeCommandJournalSpaceSyncPolicy(policy)
	if err != nil {
		return nil, err
	}

	journal.submitMu.RLock()
	defer journal.submitMu.RUnlock()
	if !journal.accepting {
		return nil, ErrCommandJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil, ErrCommandJournalClosed
	}
	if journal.spacePolicies == nil {
		journal.spacePolicies = make(map[string]CommandJournalSpaceSyncPolicy)
	}
	journal.spacePolicies[name] = normalized
	if normalized.Mode == CommandJournalSpaceSyncModePeriodic {
		journal.startPeriodicSyncLoopLocked()
		journal.signalPeriodicSyncLoopLocked()
	}
	return &CommandJournalSpace{journal: journal, name: name}, nil
}

// SetSpaceSyncPolicy changes an already registered space policy. Existing
// pending periodic bytes remain protected by the journal-wide flush loop.
func (journal *CommandJournal) SetSpaceSyncPolicy(name string, policy CommandJournalSpaceSyncPolicy) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrCommandJournalSpaceNameRequired
	}
	normalized, err := normalizeCommandJournalSpaceSyncPolicy(policy)
	if err != nil {
		return err
	}
	journal.submitMu.RLock()
	defer journal.submitMu.RUnlock()
	if !journal.accepting {
		return ErrCommandJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	if journal.spacePolicies == nil {
		journal.spacePolicies = make(map[string]CommandJournalSpaceSyncPolicy)
	}
	journal.spacePolicies[name] = normalized
	if normalized.Mode == CommandJournalSpaceSyncModePeriodic {
		journal.startPeriodicSyncLoopLocked()
		journal.signalPeriodicSyncLoopLocked()
	}
	return nil
}

// Name returns the stable logical space name.
func (space *CommandJournalSpace) Name() string {
	if space == nil {
		return ""
	}
	return space.name
}

// SyncPolicy returns the currently registered policy. A closed or nil space
// returns the safe synchronous default.
func (space *CommandJournalSpace) SyncPolicy() CommandJournalSpaceSyncPolicy {
	if space == nil || space.journal == nil {
		return CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous}
	}
	space.journal.mu.Lock()
	defer space.journal.mu.Unlock()
	if policy, ok := space.journal.spacePolicies[space.name]; ok {
		return policy
	}
	return CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous}
}

// ExecuteCommand applies one command using the space's currently registered
// policy. Read-only commands retain their normal non-journaled behavior.
func (space *CommandJournalSpace) ExecuteCommand(trie *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	if space == nil || space.journal == nil {
		return commandError(ErrNilCommandJournal.Error())
	}
	policy := space.SyncPolicy()
	return space.journal.executeCommandInSpace(trie, request, space.name, policy)
}

// Flush forces all pending periodic-space bytes to durable storage. It is a
// no-op when no periodic mutation is pending.
func (space *CommandJournalSpace) Flush() error {
	if space == nil || space.journal == nil {
		return ErrNilCommandJournal
	}
	return space.journal.flushPeriodicSync()
}

func (journal *CommandJournal) executeCommandInSpace(trie *HatTrie, request CacheCommandRequest, name string, policy CommandJournalSpaceSyncPolicy) CacheCommandResponse {
	if journal == nil {
		return commandError(ErrNilCommandJournal.Error())
	}
	if trie == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	if !commandShouldJournal(request) || policy.Mode == CommandJournalSpaceSyncModeSynchronous {
		return journal.ExecuteCommand(trie, request)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return commandError(ErrCommandJournalClosed.Error())
	}
	check, err := journal.idempotencyCheck(request)
	if err != nil {
		return commandError(err.Error())
	}
	if response, duplicate, err := journal.idempotency.lookup(check); err != nil {
		return commandError(err.Error())
	} else if duplicate {
		return response
	}

	if policy.Mode == CommandJournalSpaceSyncModeDisabled {
		response := trie.ExecuteCommand(request)
		if response.OK {
			journal.idempotency.remember(check, response, 0)
		}
		return response
	}
	if journal.periodicSyncError != nil {
		return commandError("periodic WAL sync failed: " + journal.periodicSyncError.Error())
	}

	journalRequest := journal.normalizeJournalRequest(request, trie.currentTime())
	appendState, err := journal.appendWithoutSyncLockedWithIdempotency(journalRequest, commandIdempotencyFingerprintData(check))
	if err != nil {
		return commandError(err.Error())
	}
	response := trie.ExecuteCommand(request)
	if !response.OK {
		if rollbackErr := journal.rollbackAppendWithoutSyncLocked(appendState); rollbackErr != nil {
			return commandError(response.Message + "; failed to remove rejected journal entry: " + rollbackErr.Error())
		}
		return response
	}
	journal.periodicSyncDirty = true
	journal.periodicSyncError = nil
	journal.idempotency.remember(check, response, journal.lastSequenceLocked())
	journal.notifyCommandJournalSubscriptions(CommandJournalRecord{
		Sequence: journal.lastSequenceLocked(),
		Request:  journalRequest,
	})
	journal.signalPeriodicSyncLoopLocked()
	return response
}

func (journal *CommandJournal) startPeriodicSyncLoopLocked() {
	if journal.periodicSyncStop != nil {
		return
	}
	journal.periodicSyncWake = make(chan struct{}, 1)
	journal.periodicSyncStop = make(chan struct{})
	journal.periodicSyncDone = make(chan struct{})
	go journal.runPeriodicSyncLoop()
}

func (journal *CommandJournal) signalPeriodicSyncLoopLocked() {
	if journal.periodicSyncWake == nil {
		return
	}
	select {
	case journal.periodicSyncWake <- struct{}{}:
	default:
	}
}

func (journal *CommandJournal) periodicSyncIntervalLocked() time.Duration {
	var interval time.Duration
	for _, policy := range journal.spacePolicies {
		if policy.Mode != CommandJournalSpaceSyncModePeriodic {
			continue
		}
		if interval == 0 || policy.Interval < interval {
			interval = policy.Interval
		}
	}
	return interval
}

func (journal *CommandJournal) runPeriodicSyncLoop() {
	defer close(journal.periodicSyncDone)
	for {
		journal.mu.Lock()
		interval := journal.periodicSyncIntervalLocked()
		wake := journal.periodicSyncWake
		stop := journal.periodicSyncStop
		journal.mu.Unlock()
		if interval <= 0 {
			select {
			case <-stop:
				return
			case <-wake:
			}
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
			_ = journal.flushPeriodicSync()
		}
	}
}

func (journal *CommandJournal) flushPeriodicSync() error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	if !journal.periodicSyncDirty {
		return journal.periodicSyncError
	}
	if err := journal.syncLocked(); err != nil {
		journal.periodicSyncError = err
		return err
	}
	journal.periodicSyncDirty = false
	journal.periodicSyncError = nil
	return nil
}

func (journal *CommandJournal) stopPeriodicSyncLoop() {
	if journal == nil {
		return
	}
	journal.mu.Lock()
	stop := journal.periodicSyncStop
	done := journal.periodicSyncDone
	journal.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}
