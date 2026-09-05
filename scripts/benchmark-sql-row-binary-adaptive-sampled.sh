#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryAdaptiveSampled/(full_adaptive|sampled_32|shifted_full_adaptive|shifted_sampled_32|single_shifted_legacy|single_shifted_full_adaptive|single_shifted_sampled_32)$' -benchmem -count=5
