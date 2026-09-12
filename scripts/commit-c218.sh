#!/usr/bin/env bash
set -euo pipefail

expected_base="52395e3e8655ebaf743138bba0d100527bc061d1"
if [[ "$(git rev-parse HEAD)" != "$expected_base" ]]; then
  printf 'refusing to commit: expected base %s, got %s\n' "$expected_base" "$(git rev-parse HEAD)" >&2
  exit 1
fi
git diff --check
git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	C218_WITH_FILL_INTERPOLATION.md \
	ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	WITH_FILL.md \
	hat/hatSql/c218_with_fill_interpolation.go \
  hat/hatSql/c218_with_fill_interpolation_test.go \
  hat/hatSql/with_fill.go \
  hat/hatSql/with_fill_query.go \
  scripts/benchmark-c218.sh \
  scripts/commit-c218.sh \
  scripts/format-c218.sh \
  scripts/push-c218.sh \
  scripts/status-c218.sh \
  scripts/test-c218-full.sh \
  scripts/test-c218-package.sh \
  scripts/test-c218-race.sh \
  scripts/test-c218.sh \
  scripts/vet-c218.sh
git diff --cached --check
git commit -m 'feat(hatSql): add WITH FILL interpolation policies'
