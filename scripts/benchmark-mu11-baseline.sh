#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkTypedTable.*ArrangementSnapshot' -benchmem -count=5 "$@"
