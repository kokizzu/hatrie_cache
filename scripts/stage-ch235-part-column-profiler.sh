#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH235_PART_COLUMN_PROFILER.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/query_profiler.go \
  hat/hatSql/ch235_part_column_profiler.go \
  hat/hatSql/ch235_part_column_profiler_benchmark_test.go \
  hat/hatSql/ch235_part_column_profiler_test.go \
  scripts/benchmark-ch235-part-column-profiler.sh \
  scripts/format-ch235-part-column-profiler.sh \
  scripts/race-ch235-part-column-profiler.sh \
  scripts/test-ch235-package.sh \
  scripts/test-ch235-part-column-profiler.sh \
  scripts/vet-ch235-package.sh \
  scripts/stage-ch235-part-column-profiler.sh \
  scripts/commit-ch235-part-column-profiler.sh \
  scripts/push-ch235-part-column-profiler.sh
