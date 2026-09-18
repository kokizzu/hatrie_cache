#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTR024BaselineIndexedProjection$' -benchmem -count=5
