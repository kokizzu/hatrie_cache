#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatSql ./hat/hatCache -count=1
go test -race ./hat/hatCache -run '^TestCH021OrderedLimitStopsMaterializedQueryAtLimit$' -count=1
go vet ./hat/hatSql ./hat/hatCache
test -f CH021_READ_IN_ORDER.md
rg -n -F 'C167' INSPIRATION.md
rg -n -F 'CH-021: Read-in-order early LIMIT completion' BENCHMARK.md
rg -n -F 'CH021_READ_IN_ORDER.md' INSPIRATION_BACKLOG.md
git status --short -- Makefile BENCHMARK.md INSPIRATION.md INSPIRATION_BACKLOG.md CH021_READ_IN_ORDER.md hat/hatSql/query.go hat/hatCache/ch021_read_in_order_test.go scripts/test-ch021-ordered-limit.sh scripts/benchmark-ch021-ordered-limit.sh scripts/format-ch021-ordered-limit.sh scripts/review-ch021-ordered-limit.sh scripts/commit-ch021-ordered-limit.sh scripts/push-ch021-ordered-limit.sh
