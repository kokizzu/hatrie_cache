#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH047_CSV_PARALLEL.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/ch047_csv_parallel_benchmark_test.go \
  hat/hatSql/ch047_csv_parallel_test.go \
  hat/hatSql/external_csv_parallel.go \
  scripts/benchmark-ch047-csv.sh \
  scripts/commit-ch047-csv-parallel.sh \
  scripts/format-ch047-csv.sh \
  scripts/push-ch047-csv-parallel.sh \
  scripts/race-ch047-csv.sh \
  scripts/stage-ch047-csv-parallel.sh \
  scripts/test-ch047-csv.sh
