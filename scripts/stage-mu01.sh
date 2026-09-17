#!/bin/sh
set -eu

git add Makefile MU01_DURABLE_CONNECTOR_STATE.md PRODUCT_IDEA_GAPS.md README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md hat/hatPipeline/connector_snapshot.go hat/hatPipeline/mu01_connector_snapshot_test.go hat/hatPipeline/mu01_connector_snapshot_benchmark_test.go scripts/format-mu01.sh scripts/test-mu01.sh scripts/test-mu01-verbose.sh scripts/test-mu01-package.sh scripts/test-race-mu01.sh scripts/vet-mu01.sh scripts/test-mu01-all.sh scripts/verify-mu01-docs.sh scripts/benchmark-mu01.sh scripts/review-mu01.sh scripts/stage-mu01.sh scripts/commit-mu01.sh scripts/push-mu01.sh
