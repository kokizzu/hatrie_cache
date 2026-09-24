#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM047TypedJSONSubcolumnGroupByCount$' -benchmem -count=3
