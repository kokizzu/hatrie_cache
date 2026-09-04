#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalReplayProgress$' -benchmem -benchtime=100ms -count=5
