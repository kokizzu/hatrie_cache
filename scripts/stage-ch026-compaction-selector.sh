#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	CH026_COMPACTION_SELECTORS.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	hat/hatStorage/ch026_compaction_selector_benchmark_test.go \
	hat/hatStorage/ch026_compaction_selector_test.go \
	hat/hatStorage/compaction_scheduler.go \
	scripts/benchmark-ch026-compaction-selector.sh \
	scripts/format-ch026-compaction-selector.sh \
	scripts/race-ch026-compaction-selector.sh \
	scripts/stage-ch026-compaction-selector.sh \
	scripts/test-ch026-compaction-package.sh \
	scripts/test-ch026-compaction-selector.sh \
	scripts/vet-ch026-compaction-selector.sh \
	scripts/commit-ch026-compaction-selector.sh \
	scripts/push-ch026-compaction-selector.sh
git diff --cached --check
git diff --cached --stat
