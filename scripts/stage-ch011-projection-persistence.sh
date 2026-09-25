#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  CH011_DURABLE_PROJECTIONS.md \
  Makefile \
  hat/hatSql/materialized.go \
  hat/hatSql/projection_store.go \
  hat/hatSql/session.go \
  hat/hatSql/ch011_projection_persistence_test.go \
  hat/hatSql/ch011_projection_persistence_benchmark_test.go \
  scripts/benchmark-ch011-projection-persistence.sh \
  scripts/benchmark-ch011-projection-query.sh \
  scripts/benchmark-ch011-projection-store.sh \
  scripts/format-ch011-projection-persistence.sh \
  scripts/race-ch011-projection-persistence.sh \
  scripts/stage-ch011-projection-persistence.sh \
  scripts/test-ch011-projection-persistence-package.sh \
  scripts/test-ch011-projection-persistence.sh \
  scripts/vet-ch011-projection-persistence.sh
