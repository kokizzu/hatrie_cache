#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	README.md \
	TT025_ONLINE_UNIQUENESS.md \
	hat/hatSchema/materialized.go \
	hat/hatSchema/tt025_online_uniqueness_test.go \
	hat/hatSchema/tt025_online_uniqueness_benchmark_test.go \
	scripts/test-tt025-online-uniqueness.sh \
	scripts/benchmark-tt025-online-uniqueness.sh \
	scripts/format-tt025-online-uniqueness.sh \
	scripts/race-tt025-online-uniqueness.sh \
	scripts/test-tt025-package.sh \
	scripts/verify-tt025-online-uniqueness.sh \
	scripts/stage-tt025-online-uniqueness.sh \
	scripts/commit-tt025-online-uniqueness.sh \
	scripts/push-tt025-online-uniqueness.sh
git diff --cached --check
