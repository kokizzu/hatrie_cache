#!/usr/bin/env bash
set -euo pipefail

expected_base="${EXPECTED_BASE:-603202227c071cb825227ef23ac08afa0c1357b7}"
actual_base="$(git rev-parse HEAD^0)"
if [[ "$actual_base" != "$expected_base" ]]; then
	printf 'refusing commit: expected base %s, got %s\n' "$expected_base" "$actual_base" >&2
	exit 1
fi

git add \
	BENCHMARK.md \
	C213_COMPILED_PLAN_CACHE.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatSql/c213_compiled_plan_cache.go \
	hat/hatSql/c213_compiled_plan_cache_test.go \
	hat/hatSql/c213_compiled_plan_execution.go \
	hat/hatSql/query.go \
	scripts/benchmark-c213.sh \
	scripts/commit-c213.sh \
	scripts/format-c213.sh \
	scripts/push-c213.sh \
	scripts/race-c213.sh \
	scripts/status-c213.sh \
	scripts/test-c213-full.sh \
	scripts/test-c213-package.sh \
	scripts/test-c213.sh \
	scripts/vet-c213.sh
git diff --cached --check
git commit -m 'feat(hatSql): add bounded compiled plan cache'
