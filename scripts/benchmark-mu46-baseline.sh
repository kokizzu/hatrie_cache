#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu46baseline ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialCheckpointBaseline' -benchmem -count=5
