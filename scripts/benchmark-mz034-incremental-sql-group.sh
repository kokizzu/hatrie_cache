#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ034(Rebuild|Incremental)SQLGroupAggregate$' -benchmem -count=5
