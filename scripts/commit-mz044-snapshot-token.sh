#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'Refusing to commit because the index already contains changes.' >&2
	exit 1
fi

git add \
	BENCHMARK.md \
	INSPIRATION.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	MZ044_SNAPSHOT_TOKENS.md \
	README.md \
	hat/hatCache/sql_query.go \
	hat/hatSql/keyset.go \
	hat/hatSql/mz044_snapshot_token_benchmark_test.go \
	hat/hatSql/mz044_snapshot_token_test.go \
	hat/hatSql/query.go \
	hat/hatSql/sql_snapshot_token.go \
	scripts/benchmark-mz044-snapshot-token.sh \
	scripts/commit-mz044-snapshot-token.sh \
	scripts/format-mz044-snapshot-token.sh \
	scripts/push-mz044-snapshot-token.sh \
	scripts/review-mz044-snapshot-token.sh \
	scripts/test-mz044-snapshot-token.sh \
	scripts/verify-mz044-snapshot-token.sh

git diff --cached --check
git diff --cached --name-only
git commit -m 'chore: harden snapshot token git wrappers'
