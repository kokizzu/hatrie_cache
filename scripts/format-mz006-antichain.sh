#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/antichain.go \
	hat/hatPipeline/mz006_antichain_test.go \
	hat/hatPipeline/mz006_antichain_baseline_benchmark_test.go \
	hat/hatPipeline/mz006_antichain_benchmark_test.go
