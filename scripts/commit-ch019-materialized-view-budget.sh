#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md CH019_MATERIALIZED_VIEW_BUDGET.md INSPIRATION_BACKLOG.md Makefile README.md hat/hatSql/materialized.go hat/hatSql/ch019_materialized_view_budget_test.go scripts/benchmark-ch019-materialized-view-budget.sh scripts/commit-ch019-materialized-view-budget.sh scripts/format-ch019-materialized-view-budget.sh scripts/push-ch019-materialized-view-budget.sh scripts/race-ch019-materialized-view-budget.sh scripts/test-ch019-materialized-view-budget.sh scripts/vet-ch019-materialized-view-budget.sh
git commit -m "feat: bound materialized view storage"
