#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatFiber/t235_worker_baseline_benchmark_test.go \
	hat/hatFiber/t235_worker_pool_benchmark_test.go \
	hat/hatFiber/t235_worker_pool_edge_test.go \
	hat/hatFiber/t235_worker_pool_test.go \
	hat/hatFiber/worker_pool.go
