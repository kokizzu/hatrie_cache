#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- Makefile BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md CH041_GROUPING_PLAN_SHARING.md hat/hatSql/grouping_sets.go hat/hatSql/ch041_grouping_branch_plan_test.go hat/hatSql/ch041_grouping_query_benchmark_test.go scripts/benchmark-ch041-grouping-plan-comparison.sh scripts/benchmark-ch041-grouping-plan.sh scripts/format-ch041-grouping-plan.sh scripts/race-ch041-grouping-plan.sh scripts/review-ch041-grouping-plan.sh scripts/test-ch041-grouping-plan.sh scripts/vet-ch041-grouping-plan.sh scripts/commit-ch041-grouping-plan.sh scripts/push-ch041-grouping-plan.sh
rg -n 'CH-041|grouping branch|grouping-set plan' BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md CH041_GROUPING_PLAN_SHARING.md
