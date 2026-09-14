#!/usr/bin/env bash
set -euo pipefail
git add BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md CH054_TYPED_IN_SEARCH.md hat/hatSql/in_program.go hat/hatSql/ch054_in_search_test.go scripts/benchmark-ch054-in-search.sh scripts/commit-ch054-in-search.sh scripts/format-ch054-in-search.sh scripts/push-ch054-in-search.sh scripts/race-ch054-in-search.sh scripts/test-ch054-in-search.sh scripts/vet-ch054-in-search.sh
git commit -m "sql: binary search large literal IN sets"
