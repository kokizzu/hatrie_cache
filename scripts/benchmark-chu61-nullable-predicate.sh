#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU61NullablePredicates$' -benchmem -count=5
