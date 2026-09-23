#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatJournal \
  -run '^(TestT212|TestSegmentedCommandJournal|TestJournalSegment|TestValidateCommandJournal)' \
  -count=1
