#!/usr/bin/env bash
set -eu

gofmt -w hat/hatPipeline/frontier_retention.go hat/hatPipeline/mz027_read_hold_diagnostics_test.go hat/hatPipeline/mz027_read_hold_diagnostics_benchmark_test.go
