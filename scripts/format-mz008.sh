#!/usr/bin/env bash
set -eu

gofmt -w hat/hatPipeline/frontier_antichain.go hat/hatPipeline/mz008_frontier_antichain_test.go hat/hatPipeline/mz008_frontier_antichain_benchmark_test.go
