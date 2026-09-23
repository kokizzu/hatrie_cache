#!/usr/bin/env bash
set -euo pipefail

make verify-t217-scope
paths=(
	Makefile
	README.md
	BENCHMARK.md
	INSPIRATION_ROUND2.md
	T217_COLUMNAR_BATCH_INGEST.md
	hat/hatSql/typed_table_columnar_batch.go
	hat/hatSql/t217_columnar_batch_benchmark_test.go
	hat/hatSql/t217_columnar_batch_test.go
	scripts/format-t217.sh
	scripts/test-t217.sh
	scripts/benchmark-t217-before.sh
	scripts/benchmark-t217.sh
	scripts/test-t217-package.sh
	scripts/race-t217.sh
	scripts/vet-t217.sh
	scripts/verify-t217-scope.sh
	scripts/stage-t217.sh
	scripts/commit-t217.sh
	scripts/push-t217.sh
)
git add -- "${paths[@]}"
git diff --cached --name-only
