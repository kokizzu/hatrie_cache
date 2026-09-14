#!/usr/bin/env bash
set -euo pipefail

git add Makefile hat/hatSql/contracts.go hat/hatSql/tr019_columnar_value_fastpath_test.go TR019_COLUMNAR_VALUE_FASTPATH.md INSPIRATION.md INSPIRATION_BACKLOG.md BENCHMARK.md scripts/test-tr019-columnar-value.sh scripts/benchmark-tr019-columnar-value.sh scripts/format-tr019-columnar-value.sh scripts/review-tr019-columnar-value.sh scripts/commit-tr019-columnar-value.sh scripts/push-tr019-columnar-value.sh
git diff --cached --check
git commit -m 'Optimize plain columnar value lookup'
