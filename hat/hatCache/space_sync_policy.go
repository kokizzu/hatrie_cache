package hatCache

import (
	"errors"
	"strings"
)

// CommandJournalSpaceSyncMode controls when a journaled space asks the WAL
// file to flush its bytes to stable storage.
type CommandJournalSpaceSyncMode string

const (
	// CommandJournalSpaceSyncImmediate preserves the journal's default policy.
	CommandJournalSpaceSyncImmediate CommandJournalSpaceSyncMode = "immediate"
	// CommandJournalSpaceSyncPeriodic syncs after every configured number of
	// accepted entries for the space.
	CommandJournalSpaceSyncPeriodic CommandJournalSpaceSyncMode = "periodic"
	// CommandJournalSpaceSyncDisabled skips fsync for the space; callers must
	// use a separate durability boundary before relying on those entries.
	CommandJournalSpaceSyncDisabled CommandJournalSpaceSyncMode = "disabled"
)

// CommandJournalSpaceSyncPolicy is an opt-in WAL durability policy for one
// logical space. Unconfigured spaces use immediate syncing.
type CommandJournalSpaceSyncPolicy struct {
	Mode  CommandJournalSpaceSyncMode
	Every uint64
}

func (policy CommandJournalSpaceSyncPolicy) normalized() (CommandJournalSpaceSyncPolicy, error) {
	policy.Mode = CommandJournalSpaceSyncMode(strings.ToLower(strings.TrimSpace(string(policy.Mode))))
	if policy.Mode == "" {
		policy.Mode = CommandJournalSpaceSyncImmediate
	}
	switch policy.Mode {
	case CommandJournalSpaceSyncImmediate, CommandJournalSpaceSyncDisabled:
		policy.Every = 0
		return policy, nil
	case CommandJournalSpaceSyncPeriodic:
		if policy.Every == 0 {
			return CommandJournalSpaceSyncPolicy{}, errors.New("hatriecache: periodic space sync policy requires every > 0")
		}
		return policy, nil
	default:
		return CommandJournalSpaceSyncPolicy{}, errors.New("hatriecache: unknown space sync policy mode")
	}
}

// SetSpaceSyncPolicy sets or replaces the WAL sync policy for space. An
// immediate policy removes an override and restores the default behavior.
func (journal *CommandJournal) SetSpaceSyncPolicy(space string, policy CommandJournalSpaceSyncPolicy) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return errors.New("hatriecache: space is required for a sync policy")
	}
	normalized, err := policy.normalized()
	if err != nil {
		return err
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	if normalized.Mode == CommandJournalSpaceSyncImmediate {
		delete(journal.spaceSyncPolicies, space)
		delete(journal.spaceSyncPending, space)
		return nil
	}
	if journal.spaceSyncPolicies == nil {
		journal.spaceSyncPolicies = make(map[string]CommandJournalSpaceSyncPolicy)
	}
	if journal.spaceSyncPending == nil {
		journal.spaceSyncPending = make(map[string]uint64)
	}
	journal.spaceSyncPolicies[space] = normalized
	delete(journal.spaceSyncPending, space)
	return nil
}

// SpaceSyncPolicies returns a copy of the configured per-space overrides.
func (journal *CommandJournal) SpaceSyncPolicies() map[string]CommandJournalSpaceSyncPolicy {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	policies := make(map[string]CommandJournalSpaceSyncPolicy, len(journal.spaceSyncPolicies))
	for space, policy := range journal.spaceSyncPolicies {
		policies[space] = policy
	}
	return policies
}

// Sync forces the journal's current bytes through its durability boundary.
// It is useful after disabled or periodic space policies at an application
// checkpoint or before an externally coordinated handoff.
func (journal *CommandJournal) Sync() error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	return journal.syncLocked()
}

type commandJournalSpaceSyncReservation struct {
	space           string
	previousPending uint64
	tracked         bool
	syncRequired    bool
}

// reserveSpaceSync is called while journal.mu is held. The reservation makes
// periodic cadence changes reversible if append or command execution fails.
func (journal *CommandJournal) reserveSpaceSync(space string) commandJournalSpaceSyncReservation {
	if len(journal.spaceSyncPolicies) == 0 {
		return commandJournalSpaceSyncReservation{syncRequired: true}
	}
	space = strings.TrimSpace(space)
	policy, ok := journal.spaceSyncPolicies[space]
	if !ok || policy.Mode == CommandJournalSpaceSyncImmediate {
		return commandJournalSpaceSyncReservation{syncRequired: true}
	}
	reservation := commandJournalSpaceSyncReservation{
		space:   space,
		tracked: true,
	}
	if policy.Mode == CommandJournalSpaceSyncDisabled {
		return reservation
	}
	previous := journal.spaceSyncPending[space]
	reservation.previousPending = previous
	if previous+1 >= policy.Every {
		delete(journal.spaceSyncPending, space)
		reservation.syncRequired = true
		return reservation
	}
	journal.spaceSyncPending[space] = previous + 1
	return reservation
}

func (journal *CommandJournal) rollbackSpaceSync(reservation commandJournalSpaceSyncReservation) {
	if !reservation.tracked || journal.spaceSyncPolicies[reservation.space].Mode != CommandJournalSpaceSyncPeriodic {
		return
	}
	if reservation.previousPending == 0 {
		delete(journal.spaceSyncPending, reservation.space)
		return
	}
	journal.spaceSyncPending[reservation.space] = reservation.previousPending
}

func (journal *CommandJournal) rollbackSpaceSyncReservations(reservations []commandJournalSpaceSyncReservation) {
	for index := len(reservations) - 1; index >= 0; index-- {
		journal.rollbackSpaceSync(reservations[index])
	}
}
