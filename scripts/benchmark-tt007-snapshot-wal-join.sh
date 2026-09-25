#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkTT007(JoinCommandJournalSnapshot|ManualSnapshotThenPull)$' -benchmem -count=5
