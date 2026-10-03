#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test ./hat/hatCache -run '^$' -bench '^BenchmarkT018CommandJournal(Periodic|Disabled)Write$' -benchmem -benchtime=300ms -count=3
