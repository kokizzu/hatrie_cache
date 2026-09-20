#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatFiber/scheduler.go \
	hat/hatFiber/sync.go \
	hat/hatFiber/channel.go \
	hat/hatFiber/tu31_sync_test.go \
	hat/hatFiber/tu31_sync_benchmark_test.go \
	hat/hatFiber/tu31_example_test.go
