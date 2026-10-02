#!/usr/bin/env bash
set -euo pipefail

test ! -e hat/hatSql/dataflow_ir.go
test ! -e hat/hatSql/round63_build_prereqs.go
git diff --check
git add \
	BENCHMARK.md \
	CH060_BITMAP_SECONDARY_UNION.md \
	ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	hat/hatCache/ch060_bitmap_secondary_benchmark_test.go \
	hat/hatCache/ch060_bitmap_secondary_test.go \
	hat/hatCache/sql_query.go \
	scripts/benchmark-ch060-bitmap-secondary.sh \
	scripts/commit-ch060-bitmap-secondary.sh \
	scripts/format-ch060-bitmap-secondary.sh \
	scripts/push-ch060-bitmap-secondary.sh \
	scripts/test-ch060-bitmap-secondary.sh \
	scripts/verify-ch060-bitmap-secondary.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: optimize bitmap secondary unions [skip ci]'
