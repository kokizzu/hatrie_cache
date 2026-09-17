#!/usr/bin/env bash
set -euo pipefail

count="${COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU012ExplainArrangementMetadata$' -benchmem -count="$count"
