#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ034RebuildSQLGroupAggregate$' -benchmem -count=1
