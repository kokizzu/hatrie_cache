#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile \
	INSPIRATION_BACKLOG.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	TR029_REVERSE_ITERATORS.md \
	hat/hatDataStructure/ordered_index.go \
	hat/hatDataStructure/ordered_snapshot_cursor.go \
	hat/hatDataStructure/ordered_reverse.go \
	hat/hatDataStructure/ordered_reverse_test.go \
	hat/hatDataStructure/ordered_reverse_benchmark_test.go \
	scripts/audit-inspiration.sh \
	scripts/format-tr29.sh \
	scripts/test-tr29-reverse-red.sh \
	scripts/test-tr29.sh \
	scripts/benchmark-tr29.sh \
	scripts/race-tr29.sh \
	scripts/vet-tr29.sh \
	scripts/check-tr29.sh \
	scripts/commit-tr29.sh \
	scripts/push-tr29.sh
git commit -m 'feat: add reverse ordered index iterators'
