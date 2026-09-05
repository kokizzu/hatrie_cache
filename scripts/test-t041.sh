#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatJournal ./cmd/hatrie-cache -run 'TestSegmentedCommandJournalZstdCompressionReplaysAndInspects|TestVerifyBackupPathChecksZstdSegmentedDirectoryJournal|TestParseConfigJournalSegmentCompression|TestParseConfigDefaultsJournalSegmentCompressionOff|TestParseConfigRejectsInvalidJournalSegmentCompression' -count=1
