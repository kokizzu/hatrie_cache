#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gofmt -w \
	"$root/hat/hatPipeline/frontier_registry.go" \
	"$root/hat/hatPipeline/frontier_registry_test.go" \
	"$root/hat/hatPipeline/frontier_registry_benchmark_test.go"
