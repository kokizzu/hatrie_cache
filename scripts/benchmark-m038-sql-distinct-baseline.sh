#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM038SQLIncrementalDistinct(Rebuild|Apply)$' -benchmem -count=1
