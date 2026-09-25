#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench 'BenchmarkMZ023' -benchmem -count=5 ./hat/hatSql
