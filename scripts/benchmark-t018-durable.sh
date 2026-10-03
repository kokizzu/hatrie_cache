#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test ./hat/hatCache -run '^$' -bench '^BenchmarkT018CommandJournalDefaultWrite$' -benchmem -benchtime=100ms -count=3
