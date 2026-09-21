#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  MZ016_SCHEMA_EVOLUTION.md \
  hat/hatSql/mz016_schema_evolution.go \
  hat/hatSql/mz016_schema_evolution_benchmark_test.go \
  hat/hatSql/mz016_schema_evolution_test.go \
  scripts/mz016-schema-evolution.sh \
  scripts/stage-mz016-schema-evolution.sh \
  scripts/commit-mz016-schema-evolution.sh \
  scripts/push-mz016-schema-evolution.sh
git diff --cached --check
git diff --cached --name-only
