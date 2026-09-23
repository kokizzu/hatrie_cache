#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT211JournalSync(Modes|Baseline)$' -benchmem -benchtime=20x -count=3
