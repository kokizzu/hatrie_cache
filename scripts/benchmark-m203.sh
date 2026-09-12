#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalSubscription(Replay100|SkipReplay)$' -benchtime=1s -benchmem -count=5
