#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM046JSONSubcolumnTopN$' -benchmem -count=5
