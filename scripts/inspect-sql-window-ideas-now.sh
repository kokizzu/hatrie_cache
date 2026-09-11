#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Incremental window and distinct implementation references:'
git show HEAD:hat/hatSql/incremental_frame_window.go
git show HEAD:hat/hatSql/m065i_incremental_distinct_frame_window_benchmark_test.go
git show HEAD:hat/hatSql/m065m_incremental_range_window_benchmark_test.go
git show HEAD:hat/hatSql/m065m_incremental_range_window_test.go
