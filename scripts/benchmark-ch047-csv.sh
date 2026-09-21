#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH047CSV(Serial|Parallel)(Import|Parse)$' -benchmem -count=3
