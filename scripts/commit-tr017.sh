#!/usr/bin/env bash
set -eu

git diff --check
git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	TR017_SINGLE_SOURCE_ROW_FASTPATH.md \
	hat/hatSql/query.go \
	hat/hatSql/tr017_single_source_execrow_benchmark_test.go \
	hat/hatSql/tr017_single_source_execrow_test.go \
	scripts/benchmark-tr017.sh \
	scripts/format-tr017.sh
git add scripts/commit-tr017.sh scripts/push-tr017.sh
git commit --amend --no-edit
