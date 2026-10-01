#!/usr/bin/env bash
set -euo pipefail

branch=${1:-codex/next-inspiration-round26}

if git rev-parse --verify "refs/heads/$branch" >/dev/null 2>&1; then
  git switch "$branch"
else
  git switch -c "$branch"
fi

git diff --check
git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  M-G07_SOURCE_HEALTH.md \
  Makefile \
  hat/hatCache/monitoring.go \
  hat/hatCache/source_health_monitoring.go \
  hat/hatCache/source_health_monitoring_test.go \
  hat/hatMetrics/source_health.go \
  hat/hatMetrics/source_health_benchmark_test.go \
  hat/hatMetrics/source_health_test.go \
  hat/hatMetrics/source_health_unknown_test.go \
  scripts/benchmark-mg07-source-health-compare.sh \
  scripts/format-mg07-source-health.sh \
  scripts/review-mg07-source-health.sh \
  scripts/run-mg07-source-health-cache.sh \
  scripts/run-mg07-source-health.sh \
  scripts/ship-mg07-source-health.sh \
  scripts/test-mg07-source-health-packages.sh
git diff --cached --check
git commit -m 'feat(monitoring): add bounded source health records [skip ci]'
git push -u origin "$branch"
