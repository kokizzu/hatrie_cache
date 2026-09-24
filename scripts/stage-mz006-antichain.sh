#!/usr/bin/env bash
set -euo pipefail

git add \
	ENGINE_IDEAS.md \
	Makefile \
	MZ006_ANTICHAIN_TIMESTAMPS.md \
	hat/hatPipeline/antichain.go \
	hat/hatPipeline/mz006_antichain_baseline_benchmark_test.go \
	hat/hatPipeline/mz006_antichain_benchmark_test.go \
	hat/hatPipeline/mz006_antichain_test.go \
	scripts/benchmark-mz006-antichain-baseline.sh \
	scripts/benchmark-mz006-antichain.sh \
	scripts/format-mz006-antichain.sh \
	scripts/test-mz006-antichain.sh \
	scripts/verify-mz006-antichain.sh \
	scripts/stage-mz006-antichain.sh \
	scripts/commit-mz006-antichain.sh \
	scripts/push-mz006-antichain.sh
