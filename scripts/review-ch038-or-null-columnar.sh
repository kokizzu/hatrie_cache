#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '--- worktree status ---'
git status --short
printf '%s\n' '--- feature diff stat ---'
git diff --stat -- \
	BENCHMARK.md \
	CH038_OR_NULL_COLUMNAR.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	ENGINE_IDEAS.md \
	README.md \
	hat/hatSql/query.go \
	hat/hatSql/hash_group_aggregate.go \
	hat/hatSql/ch038_or_null_columnar_test.go \
	scripts/test-ch038-or-null-columnar.sh \
	scripts/test-ch038-or-null-columnar-package.sh \
	scripts/benchmark-ch038-or-null-columnar.sh \
	scripts/format-ch038-or-null-columnar.sh \
	scripts/race-ch038-or-null-columnar.sh \
	scripts/vet-ch038-or-null-columnar.sh \
	scripts/verify-ch038-or-null-columnar-docs.sh \
	scripts/review-ch038-or-null-columnar.sh
printf '%s\n' '--- production diff ---'
git diff -- hat/hatSql/query.go hat/hatSql/hash_group_aggregate.go
