#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/sql_index_progress.go \
	hat/hatCache/sql_index_progress_benchmark_test.go \
	hat/hatCache/sql_index_progress_test.go \
	sql_index_progress_api.go
