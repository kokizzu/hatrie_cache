#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	MZ009_VALIDITY_INDEX.md \
	hat/hatSql/mz009_validity_index.go \
	hat/hatSql/mz009_validity_index_benchmark_test.go \
	hat/hatSql/mz009_validity_index_test.go \
	hat/hatSql/query.go \
	scripts/commit-mz009-validity-index.sh \
	scripts/mz009-validity-index.sh \
	scripts/push-mz009-validity-index.sh \
	scripts/stage-mz009-validity-index.sh
git diff --cached --check
git diff --cached --name-only
