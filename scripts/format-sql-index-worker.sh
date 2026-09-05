#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/sql_index_worker.go \
	hat/hatCache/sql_index_worker_test.go \
	hat/hatCache/sql_index_worker_benchmark_test.go \
	sql_index_worker_api.go
