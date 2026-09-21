#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/connector_lifecycle.go \
	hat/hatPipeline/mz048_connector_secret_rotation_baseline_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_benchmark_test.go \
	hat/hatPipeline/mz048_connector_secret_rotation_test.go
