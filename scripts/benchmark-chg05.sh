#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkCHG05(Group|WithTotals)$' -benchmem -count=5
