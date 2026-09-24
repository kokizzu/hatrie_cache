#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkTT029' -benchmem -count=5
