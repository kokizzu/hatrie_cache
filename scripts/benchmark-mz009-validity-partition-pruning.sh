#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ009ValidityPartitionPruning(Baseline|FastPath)$' -benchmem -count=5
