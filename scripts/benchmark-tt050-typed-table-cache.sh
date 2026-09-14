#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableStats$' -benchmem -count=7
