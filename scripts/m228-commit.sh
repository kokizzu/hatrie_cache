#!/usr/bin/env bash
set -euo pipefail

git commit -m "feat: add adaptive runtime join bloom precheck" -- \
  BENCHMARK.md \
  IDEA_GAP_CATALOG.md \
  Makefile \
  hat/hatSql/c212_hash_join.go \
  hat/hatSql/ch_g04_runtime_bloom_test.go \
  hat/hatSql/ch_g04_runtime_bloom_benchmark_test.go \
  scripts/m228-ch-g04-benchmark.sh \
  scripts/m228-ch-g04-compare.sh \
  scripts/m228-ch-g04-compare-1cpu.sh \
  scripts/m228-ch-g04-format.sh \
  scripts/m228-ch-g04-fpr.sh \
  scripts/m228-ch-g04-full-test.sh \
  scripts/m228-ch-g04-join-test.sh \
  scripts/m228-ch-g04-race.sh \
  scripts/m228-ch-g04-test.sh \
  scripts/m228-ch-g04-vet.sh \
  scripts/m228-commit.sh \
  scripts/m228-push.sh \
  scripts/m228-stage.sh
