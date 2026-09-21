#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	TT003_FAILOVER_ROUTE_CACHE.md \
	hat/hatReplication/tt003_route_cache.go \
	hat/hatReplication/tt003_route_cache_baseline_benchmark_test.go \
	hat/hatReplication/tt003_route_cache_benchmark_test.go \
	hat/hatReplication/tt003_route_cache_test.go \
	scripts/benchmark-tt003-route-cache-baseline.sh \
	scripts/benchmark-tt003-route-cache.sh \
	scripts/commit-tt003-route-cache.sh \
	scripts/format-tt003-route-cache.sh \
	scripts/push-tt003-route-cache.sh \
	scripts/race-tt003-route-cache.sh \
	scripts/review-tt003-route-cache.sh \
	scripts/run-tt003-route-cache-benchmark.sh \
	scripts/stage-tt003-route-cache.sh \
	scripts/test-tt003-route-cache.sh \
	scripts/verify-tt003-route-cache.sh
git diff --cached --check
