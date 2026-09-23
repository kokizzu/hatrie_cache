#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/typed_table_columnar_batch.go \
	hat/hatSql/t217_columnar_batch_benchmark_test.go \
	hat/hatSql/t217_columnar_batch_test.go
