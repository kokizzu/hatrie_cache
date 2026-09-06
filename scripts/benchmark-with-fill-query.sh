#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQL(With|Without)Fill$' -benchmem -count=1
