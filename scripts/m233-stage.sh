#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md IDEA_GAP_CATALOG.md \
  MZ038_SOURCE_LAG_ALERTS.md \
  hat/hatPipeline/mz038_source_lag_alert.go \
  hat/hatPipeline/mz038_source_lag_alert_test.go \
  hat/hatPipeline/mz038_source_lag_alert_benchmark_test.go \
  scripts/m233-mz-g38-test.sh scripts/m233-mz-g38-format.sh \
  scripts/m233-mz-g38-benchmark.sh scripts/m233-mz-g38-race.sh \
  scripts/m233-mz-g38-vet.sh scripts/m233-mz-g38-package-test.sh \
  scripts/m233-mz-g38-docs.sh scripts/m233-stage.sh \
  scripts/m233-commit.sh scripts/m233-push.sh

git diff --cached --check
git diff --cached --stat
