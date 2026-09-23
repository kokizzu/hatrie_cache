#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./hat/hatJournal \
  -run '^(TestT212|TestSegmentedCommandJournal|TestJournalSegment|TestValidateCommandJournal)' \
  -count=1
