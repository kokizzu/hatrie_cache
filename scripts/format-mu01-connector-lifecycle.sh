#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gofmt -w \
	"$root/hat/hatPipeline/connector_lifecycle.go" \
	"$root/hat/hatPipeline/connector_lifecycle_test.go" \
	"$root/hat/hatPipeline/connector_lifecycle_benchmark_test.go"
