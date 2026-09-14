#!/usr/bin/env bash
set -euo pipefail

git add Makefile BENCHMARK.md INSPIRATION.md INSPIRATION_BACKLOG.md CH021_READ_IN_ORDER.md hat/hatSql/query.go hat/hatCache/ch021_read_in_order_test.go scripts/test-ch021-ordered-limit.sh scripts/benchmark-ch021-ordered-limit.sh scripts/format-ch021-ordered-limit.sh scripts/review-ch021-ordered-limit.sh scripts/commit-ch021-ordered-limit.sh scripts/push-ch021-ordered-limit.sh
git commit -m "Stop ordered SQL scans at LIMIT"
