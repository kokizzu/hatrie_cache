#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache . -run '^TestCommandJournalReplayWithProgressReports(Completion|TargetError)$|^$' -count=1
