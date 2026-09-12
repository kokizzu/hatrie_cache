#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp)
trap 'rm -f "$output"' EXIT
go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalWriteSnapshot$' -benchmem -count=5 >"$output"
rg '^BenchmarkCommandJournalWriteSnapshot' "$output"
