#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	TT046_MEMORY_ACCOUNTING.md \
	hat/hatCache/memory_accounting.go \
	hat/hatCache/memory_compaction.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt046_memory_accounting_baseline_test.go \
	hat/hatCache/tt046_memory_accounting_test.go \
	scripts/benchmark-tt046-memory-accounting.sh \
	scripts/amend-tt046-memory-accounting.sh \
	scripts/commit-tt046-memory-accounting.sh \
	scripts/format-tt046-memory-accounting.sh \
	scripts/race-tt046-memory-accounting.sh \
	scripts/review-tt046-memory-accounting.sh \
	scripts/test-tt046-memory-accounting.sh \
	scripts/verify-tt046-docs.sh \
	scripts/vet-tt046-memory-accounting.sh \
	scripts/push-tt046-memory-accounting.sh
git diff --cached --check
git commit -m "feat: add per-structure memory accounting"
