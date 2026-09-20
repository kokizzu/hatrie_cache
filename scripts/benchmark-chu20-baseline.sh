#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU20(JSONQueryRowFallback|JSONExistsRowFallback)$' -benchmem -count=5
