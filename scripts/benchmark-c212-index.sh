#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkC212(CanonicalMap|JoinIndex)(Numeric|String)$' -benchmem -count=5 ./hat/hatSql
