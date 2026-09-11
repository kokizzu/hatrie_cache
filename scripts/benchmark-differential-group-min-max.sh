#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialGroupMinMax/(naive_rebuild|incremental)$' -benchmem -count=5 -cpu=1
