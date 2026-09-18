#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH012_PROJECTION_ADVISOR_COST.md \
  hat/hatSql/projection_advisor.go \
  hat/hatSql/ch012_projection_advisor_cost_test.go \
  hat/hatSql/ch012_projection_advisor_cost_benchmark_test.go \
  scripts/format-ch012-projection-advisor-cost.sh \
  scripts/test-ch012-projection-advisor-cost.sh \
  scripts/benchmark-ch012-projection-advisor-cost.sh \
  scripts/review-ch012.sh \
  scripts/stage-ch012-projection-advisor-cost.sh \
  scripts/commit-ch012-projection-advisor-cost.sh \
  scripts/push-ch012-projection-advisor-cost.sh

test ! -e scripts/inspect-ch012-context.sh
test ! -e scripts/inspect-ch012-doc-context.sh
git diff --stat -- \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH012_PROJECTION_ADVISOR_COST.md \
  hat/hatSql/projection_advisor.go \
  hat/hatSql/ch012_projection_advisor_cost_test.go \
  hat/hatSql/ch012_projection_advisor_cost_benchmark_test.go \
  scripts/format-ch012-projection-advisor-cost.sh \
  scripts/test-ch012-projection-advisor-cost.sh \
  scripts/benchmark-ch012-projection-advisor-cost.sh \
  scripts/review-ch012.sh \
  scripts/stage-ch012-projection-advisor-cost.sh \
  scripts/commit-ch012-projection-advisor-cost.sh \
  scripts/push-ch012-projection-advisor-cost.sh
git status --short --untracked-files=all
