#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatJournal ./hat/hatCache -run '^(TestTTG11|TestSegmentedCommandJournalZstdCompressionReplaysAndInspects)$' -count=1
