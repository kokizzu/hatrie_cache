#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH011_PROJECTION_DDL.md \
  hat/hatSql/session.go \
  hat/hatSql/materialized.go \
  hat/hatSql/ch011_projection_ddl_test.go \
  hat/hatSql/ch011_projection_ddl_benchmark_test.go \
  scripts/format-ch011-projection-ddl.sh \
  scripts/test-ch011-projection-ddl.sh \
  scripts/benchmark-ch011-projection-ddl.sh \
  scripts/review-ch011.sh \
  scripts/stage-ch011-projection-ddl.sh \
  scripts/commit-ch011-projection-ddl.sh \
  scripts/push-ch011-projection-ddl.sh
git diff --cached --check
printf '%s\n' 'Staged CH-011 paths:'
git diff --cached --name-only
