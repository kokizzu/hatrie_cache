#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ENGINE_IDEAS.md \
  Makefile \
  TT029_BEFORE_TRIGGERS.md \
  hat/hatSql/sql_trigger_definition.go \
  hat/hatSql/sql_trigger_definition_test.go \
  hat/hatSql/sql_triggers.go \
  hat/hatSql/tt029_before_trigger_baseline_benchmark_test.go \
  hat/hatSql/tt029_before_trigger_test.go \
  scripts/benchmark-tt029-before-trigger.sh \
  scripts/format-tt029-before-trigger.sh \
  scripts/stage-tt029-before-trigger.sh \
  scripts/test-tt029-before-trigger.sh \
  scripts/verify-tt029-before-trigger.sh \
  scripts/commit-tt029-before-trigger.sh \
  scripts/push-tt029-before-trigger.sh
