#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCommandJournalReplayWithProgressReports(Completion|TargetError)$' -count=1
go test . -run '^$' -count=1
