#!/usr/bin/env bash
set -euo pipefail

count="${1:-1}"
pattern="${2:-^BenchmarkConflictPolicyResolution$}"
go test ./hat/hatReplication -run '^$' -bench "$pattern" -benchmem -count="$count"
