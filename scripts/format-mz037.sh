#!/bin/sh
set -eu

gofmt -w \
	hat/hatPipeline/async_batcher.go \
	hat/hatPipeline/worker_local_exchange.go \
	hat/hatPipeline/mz037_worker_local_exchange_test.go \
	hat/hatPipeline/mz037_worker_local_exchange_benchmark_test.go
