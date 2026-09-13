#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_WINDOW_FRAME_EXCLUSION.md hat/hatSql/query.go hat/hatSql/window_exclusion_test.go scripts/benchmark-ch042-after.sh scripts/benchmark-ch042-before.sh scripts/check-ch042.sh scripts/commit-ch042.sh scripts/format-ch042.sh scripts/push-ch042.sh scripts/race-ch042.sh scripts/review-ch042.sh scripts/test-ch042-all.sh scripts/test-ch042-repo.sh scripts/test-ch042.sh scripts/vet-ch042.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add SQL window frame exclusion"
