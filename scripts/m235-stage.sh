#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md IDEA_GAP_CATALOG.md CH050_PLAN_REPRODUCIBILITY_HASH.md \
  hat/hatSql/ch050_plan_reproducibility_hash.go \
  hat/hatSql/ch050_plan_reproducibility_hash_test.go \
  hat/hatSql/ch050_plan_reproducibility_hash_benchmark_test.go \
  scripts/m235-ch-g50-test.sh scripts/m235-ch-g50-format.sh \
  scripts/m235-ch-g50-benchmark.sh scripts/m235-ch-g50-race.sh \
  scripts/m235-ch-g50-vet.sh scripts/m235-ch-g50-package-test.sh \
  scripts/m235-ch-g50-docs.sh scripts/m235-stage.sh scripts/m235-commit.sh scripts/m235-push.sh
git diff --cached --check
git diff --cached --stat
