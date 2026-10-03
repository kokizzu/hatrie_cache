package hatriecache

import "hatrie_cache/hat/hatCache"

// CommandJournalSyncMode is the root-package compatibility alias for the
// journal durability policy.
type CommandJournalSyncMode = hatCache.CommandJournalSyncMode

const (
	CommandJournalSyncModeDurable     = hatCache.CommandJournalSyncModeDurable
	CommandJournalSyncModePeriodic    = hatCache.CommandJournalSyncModePeriodic
	CommandJournalSyncModeNone        = hatCache.CommandJournalSyncModeNone
	DefaultCommandJournalSyncMode     = hatCache.DefaultCommandJournalSyncMode
	DefaultCommandJournalSyncInterval = hatCache.DefaultCommandJournalSyncInterval
	MinCommandJournalSyncInterval     = hatCache.MinCommandJournalSyncInterval
	MaxCommandJournalSyncInterval     = hatCache.MaxCommandJournalSyncInterval
)

// ParseCommandJournalSyncMode parses the root-package journal durability
// policy while preserving the hatCache compatibility API.
func ParseCommandJournalSyncMode(value string) (CommandJournalSyncMode, error) {
	return hatCache.ParseCommandJournalSyncMode(value)
}
