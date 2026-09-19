#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/mu048_connector_health.go \
	hat/hatPipeline/mu048_connector_health_test.go \
	hat/hatPipeline/mu048_connector_health_benchmark_test.go \
