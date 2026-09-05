#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./hat/hatJournal ./cmd/hatrie-cache -run 'TestSegmentedCommandJournalZstdCompressionReplaysAndInspects|TestVerifyBackupPathChecksZstdSegmentedDirectoryJournal|TestParseConfigJournalSegmentCompression|TestParseConfigDefaultsJournalSegmentCompressionOff|TestParseConfigRejectsInvalidJournalSegmentCompression' -count=1
