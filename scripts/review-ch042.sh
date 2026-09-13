#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat
grep -n -E 'SQL_WINDOW_FRAME_EXCLUSION|CH-042|EXCLUDE CURRENT ROW|BenchmarkSQLWindowFrameExclusion' README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_WINDOW_FRAME_EXCLUSION.md
