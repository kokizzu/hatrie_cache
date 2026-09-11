#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SQL_ARG_EXTREME.md hat/hatSql/query.go hat/hatSql/arg_extreme.go hat/hatSql/arg_extreme_test.go scripts/test-ch040-arg-extreme.sh scripts/benchmark-ch040-arg-extreme.sh scripts/format-ch040-arg-extreme.sh scripts/test-ch040-sql-package.sh scripts/test-race-ch040-arg-extreme.sh scripts/vet-ch040-arg-extreme.sh scripts/verify-ch040-docs.sh scripts/review-ch040-arg-extreme.sh scripts/commit-ch040-arg-extreme.sh scripts/push-ch040-arg-extreme.sh
git diff --cached --check
git commit -m 'hatSql: add argMax and argMin aggregates'
