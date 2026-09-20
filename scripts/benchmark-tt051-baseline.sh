#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT051GlobalSynchronousJournal$' -benchmem -benchtime="${TT051_BENCHTIME:-1s}" -count=5
