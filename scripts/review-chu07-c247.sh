#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
	Makefile \
	README.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	CHU07_MUTATION_LIFECYCLE.md \
	hat/hatCache/async_command.go \
	hat/hatCache/chu07_mutation_status.go \
	hat/hatCache/chu07_mutation_lifecycle_test.go \
	hat/hatCache/chu07_mutation_lifecycle_benchmark_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/system_tables.go \
	scripts/benchmark-chu07-c247.sh \
	scripts/format-chu07-c247.sh \
	scripts/memory-chu07-c247.sh \
	scripts/race-chu07-c247.sh \
	scripts/test-chu07-c247.sh \
	scripts/test-chu07-package-c247.sh \
	scripts/test-chu07-repo-c247.sh \
	scripts/verify-chu07-docs-c247.sh \
	scripts/vet-chu07-c247.sh
printf '%s\n' '--- CH-U07 Makefile hunks ---'
git diff -- Makefile | rg -n -C 5 'chu07-c247' || true
printf '%s\n' '--- temporary target locations ---'
rg -n -C 3 'inspect-chu07-running|review-chu07-c247|test-chu07-package-c247|vet-chu07-c247' Makefile
printf '%s\n' '--- production diff ---'
git diff --unified=3 -- \
	hat/hatCache/async_command.go \
	hat/hatCache/chu07_mutation_status.go \
	hat/hatCache/journal.go \
	hat/hatCache/system_tables.go | sed -n '1,520p'
sed -n '100,190p' hat/hatCache/chu07_mutation_lifecycle_test.go
