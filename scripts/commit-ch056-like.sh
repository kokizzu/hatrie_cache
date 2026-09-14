#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md CH056_PREPARED_LIKE.md \
  hat/hatSql/query.go hat/hatSql/like_program.go hat/hatSql/ch056_like_program_test.go \
  scripts/benchmark-ch056-like.sh scripts/format-ch056-like.sh scripts/race-ch056-like.sh \
  scripts/test-ch056-like.sh scripts/vet-ch056-like.sh scripts/commit-ch056-like.sh \
  scripts/push-ch056-like.sh
git commit -m "sql: prepare literal LIKE patterns"
