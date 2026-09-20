#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatFiber/local.go \
	hat/hatFiber/scheduler.go \
	hat/hatFiber/tu32_example_test.go \
	hat/hatFiber/tu32_local_test.go \
	hat/hatFiber/tu32_local_benchmark_test.go
