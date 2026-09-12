#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournal(PointReadThenSequence|ReadFence)$' -benchtime=1s -benchmem -count=5
