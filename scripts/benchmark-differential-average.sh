#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkGroupAverageInt64DifferentialRows/(before_separate_count_sum|before_combined_count_sum|after_average)$' -benchtime=200ms -count=7
