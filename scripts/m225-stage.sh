#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M225_PERSISTED_SHARD_LEASES.md \
  README.md \
  Makefile \
  hat/hatSql/m225_shard_leases.go \
  hat/hatSql/m225_shard_leases_benchmark_test.go \
  hat/hatSql/m225_shard_leases_test.go \
  scripts/m225-benchmark.sh \
  scripts/m225-format.sh \
  scripts/m225-race.sh \
  scripts/m225-status.sh \
  scripts/m225-stage.sh \
  scripts/m225-test.sh \
  scripts/m225-vet.sh \
  scripts/m225-commit.sh \
  scripts/m225-push.sh
git diff --cached --check
git diff --cached --stat
