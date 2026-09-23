#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatJournal \
  -run '^(TestT213|TestSnapshot|TestSaveSnapshot|TestLoadSnapshot|TestWriteSnapshot|TestReadSnapshot|TestVerifySnapshot|TestCommandJournalSourceCheckpoint)' \
  -count=1
