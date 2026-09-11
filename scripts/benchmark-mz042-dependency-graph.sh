#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ042MaterializedRefreshChanged$' -benchmem -benchtime=1s -count=5
