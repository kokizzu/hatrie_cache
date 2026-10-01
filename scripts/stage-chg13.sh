#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md BENCHMARK.md TU12_AUTOMATIC_FAILOVER_PLANNER.md \
	hat/hatTopology/failover.go hat/hatTopology/failover_test.go hat/hatTopology/failover_benchmark_test.go hat/hatTopology/failover_large_test.go \
	scripts/benchmark-chg13.sh scripts/check-chg13.sh scripts/format-chg13.sh scripts/race-chg13.sh scripts/test-chg13.sh scripts/test-chg13-package.sh scripts/vet-chg13.sh \
	scripts/status-chg13.sh scripts/stage-chg13.sh scripts/commit-chg13.sh scripts/push-chg13.sh
