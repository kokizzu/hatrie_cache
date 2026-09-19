#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu46 ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialCheckpointHDF1' -benchmem -count=5
