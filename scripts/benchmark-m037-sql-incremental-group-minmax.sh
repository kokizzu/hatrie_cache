#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM037SQLIncrementalGroupMinMax(Rebuild|Apply)$' -benchmem -count=1
