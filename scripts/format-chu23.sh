#!/bin/sh
set -eu
gofmt -w hat/hatCache/chu23_async_insert_submit_benchmark_test.go hat/hatCache/chu23_async_insert_queue_benchmark_test.go hat/hatCache/chu23_async_insert_queue_test.go
gofmt -w hat/hatCache/async_insert_queue.go hat/hatCache/async_insert_queue_http.go hat/hatCache/chu23_async_insert_submit_benchmark_test.go hat/hatCache/chu23_async_insert_queue_benchmark_test.go hat/hatCache/chu23_async_insert_queue_test.go hat/hatCache/monitoring.go api.go
