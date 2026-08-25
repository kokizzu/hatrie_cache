package hatriecache

import "hatrie_cache/hat/hatJournal"

// CommandJournalFormat is retained for compatibility. New integrations can
// import hat/hatJournal directly.
type CommandJournalFormat = hatJournal.Format

const (
	CommandJournalFormatJSON    = hatJournal.FormatJSON
	CommandJournalFormatBinary  = hatJournal.FormatBinary
	DefaultCommandJournalFormat = hatJournal.DefaultFormat
)

// ParseCommandJournalFormat is retained for compatibility. New integrations
// can use hatJournal.ParseFormat directly.
func ParseCommandJournalFormat(value string) (CommandJournalFormat, error) {
	return hatJournal.ParseFormat(value)
}

const (
	DefaultJournalGroupCommitWindow       = hatJournal.DefaultGroupCommitWindow
	DefaultJournalGroupCommitMaxBatch     = hatJournal.DefaultGroupCommitMaxBatch
	MaxJournalGroupCommitBatch            = hatJournal.MaxGroupCommitBatch
	DefaultCommandJournalSegmentMaxBytes  = hatJournal.DefaultSegmentMaxBytes
	DefaultCommandJournalRetainedSegments = hatJournal.DefaultRetainedSegments
	MaxCommandJournalRetainedSegments     = hatJournal.MaxRetainedSegments
)

// CommandJournalOptions is retained for compatibility. New integrations can
// import hat/hatJournal directly.
type CommandJournalOptions = hatJournal.Options

// InspectCommandJournal validates a journal without modifying it. It combines
// portable framing and sequence inspection with cache-specific command
// validation, so a successful report is safe to use as a restore preflight.
func InspectCommandJournal(path string, options CommandJournalOptions) (hatJournal.Inspection, error) {
	normalized, err := hatJournal.ValidateOptions(options)
	if err != nil {
		return hatJournal.Inspection{}, err
	}
	inspection, err := hatJournal.Inspect(path, hatJournal.InspectOptions{
		Segmented: normalized.SegmentMaxBytes > 0,
	})
	if err != nil {
		return hatJournal.Inspection{}, err
	}
	if _, err := scanCommandJournalSet(path, normalized.SegmentMaxBytes > 0, nil); err != nil {
		return hatJournal.Inspection{}, err
	}
	return inspection, nil
}
