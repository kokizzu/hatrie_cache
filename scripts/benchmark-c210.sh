#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkTypedTableStats|BenchmarkTypedTableHistogram)$' -benchmem -count=5
