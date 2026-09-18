#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTR024(BaselineIndexedProjection|CoveringIndexedProjection)$' -benchmem -count=5
