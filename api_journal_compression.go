package hatriecache

import core "hatrie_cache/hat/hatCache"

type CommandJournalSegmentCompression = core.CommandJournalSegmentCompression

const (
	CommandJournalSegmentCompressionNone    = core.CommandJournalSegmentCompressionNone
	CommandJournalSegmentCompressionZstd    = core.CommandJournalSegmentCompressionZstd
	DefaultCommandJournalSegmentCompression = core.DefaultCommandJournalSegmentCompression
)

var ParseCommandJournalSegmentCompression = core.ParseCommandJournalSegmentCompression
