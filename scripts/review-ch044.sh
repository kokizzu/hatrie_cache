#!/bin/sh
set -eu
git diff --check
git status --short
git diff --stat
git diff -- \
  ENGINE_IDEAS.md \
  README.md \
  BENCHMARK.md \
  CH044_JSON_DYNAMIC_SUBCOLUMNS.md \
  Makefile \
  hat/hatCache/main.go \
  hat/hatCache/sql_json_subcolumn.go \
  hat/hatCache/ch044_json_subcolumn_test.go \
  hat/hatCache/ch044_json_subcolumn_benchmark_test.go \
  hat/hatSql/ch031_automatic_json_subcolumn.go \
  scripts/test-ch044.sh \
  scripts/benchmark-ch044.sh \
  scripts/format-ch044.sh \
  scripts/test-ch044-package.sh \
  scripts/race-ch044.sh \
  scripts/vet-ch044.sh
