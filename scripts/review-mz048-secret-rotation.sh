#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	MZ048_CONNECTOR_SECRET_ROTATION.md \
	Makefile \
	README.md \
	hat/hatPipeline/connector_lifecycle.go \
	hat/hatPipeline/mz048_connector_secret_rotation_baseline_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_test.go \
	scripts/benchmark-mz048-secret-rotation-baseline.sh \
	scripts/benchmark-mz048-secret-rotation.sh \
	scripts/commit-mz048-secret-rotation.sh \
	scripts/format-mz048-secret-rotation.sh \
	scripts/push-mz048-secret-rotation.sh \
	scripts/race-mz048-secret-rotation.sh \
	scripts/review-mz048-secret-rotation.sh \
	scripts/run-mz048-secret-rotation-benchmark.sh \
	scripts/stage-mz048-secret-rotation.sh \
	scripts/test-mz048-secret-rotation.sh \
	scripts/verify-mz048-secret-rotation.sh
git status --short
git diff --stat -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	MZ048_CONNECTOR_SECRET_ROTATION.md \
	Makefile \
	README.md \
	hat/hatPipeline/connector_lifecycle.go \
	hat/hatPipeline/mz048_connector_secret_rotation_baseline_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_test.go \
	scripts/benchmark-mz048-secret-rotation-baseline.sh \
	scripts/benchmark-mz048-secret-rotation.sh \
	scripts/format-mz048-secret-rotation.sh \
	scripts/race-mz048-secret-rotation.sh \
	scripts/review-mz048-secret-rotation.sh \
	scripts/run-mz048-secret-rotation-benchmark.sh \
	scripts/stage-mz048-secret-rotation.sh \
	scripts/test-mz048-secret-rotation.sh \
	scripts/verify-mz048-secret-rotation.sh
