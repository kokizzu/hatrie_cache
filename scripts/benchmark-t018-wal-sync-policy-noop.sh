#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test ./hat/hatCache -run '^$' -bench '^BenchmarkT018CommandJournalDefaultWriteNoopSync$' -benchmem -benchtime=1s -count=5
