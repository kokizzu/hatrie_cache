#!/usr/bin/env bash
set -euo pipefail
git add \
  CH030_EXECUTION_STEPS.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/ch030_execution_steps_baseline_benchmark_test.go \
  hat/hatSql/ch030_execution_steps_benchmark_test.go \
  hat/hatSql/ch030_execution_steps_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_result_cache.go \
  scripts/benchmark-ch030-execution-steps-baseline.sh \
  scripts/benchmark-ch030-execution-steps.sh \
  scripts/commit-ch030-execution-steps.sh \
  scripts/format-ch030-execution-steps.sh \
  scripts/push-ch030-execution-steps.sh \
  scripts/stage-ch030-execution-steps.sh \
  scripts/test-ch030-execution-steps.sh \
  scripts/verify-ch030-execution-steps.sh
git diff --cached --stat
