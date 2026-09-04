#!/usr/bin/env bash
set -euo pipefail

go test . ./hat/hatJournal ./hat/hatCache -run 'TestCommandJournalRetainedBytesConstants|TestValidateOptionsRetainedBytes|TestSegmentedCommandJournalRetainsWithinByteBudget|TestSegmentedCommandJournalPrunesOldestWholeSegments|TestSegmentedCommandJournalPinsUnacknowledgedOutboxRecords' -count=1
go test ./cmd/hatrie-cache -run 'TestParseConfigJournalGroupCommitDefaultsAndOverrides|TestParseConfigRejectsInvalidJournalGroupCommit' -count=1
go test ./hat/hatCache -run '^TestMonitoringWrapperPassesJournalGroupCommitDefaults$' -count=1
