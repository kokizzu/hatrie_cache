#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU58CompositeSparseMarkQuery(Baseline)?$' -benchmem -count=5
