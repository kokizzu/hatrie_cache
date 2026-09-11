#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat
git diff -- Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md hat/hatSql/incremental_range_window.go hat/hatSql/m065m_incremental_range_window_test.go hat/hatSql/m065m_incremental_range_window_benchmark_test.go
