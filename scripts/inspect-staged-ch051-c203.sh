#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --stat -- \
	Makefile \
	CH051_LOW_CARDINALITY.md \
	BENCHMARK.md \
	README.md \
	INSPIRATION_BACKLOG.md \
	hat/hatDataStructure/low_cardinality.go \
	hat/hatDataStructure/low_cardinality_test.go \
	hat/hatDataStructure/low_cardinality_benchmark_test.go \
	scripts/test-ch051-c203.sh \
	scripts/format-ch051-c203.sh \
	scripts/benchmark-ch051-c203.sh \
	scripts/verify-ch051-c203.sh \
	scripts/stage-ch051-c203.sh \
	scripts/inspect-staged-ch051-c203.sh \
	scripts/commit-ch051-c203.sh \
	scripts/push-ch051-c203.sh
