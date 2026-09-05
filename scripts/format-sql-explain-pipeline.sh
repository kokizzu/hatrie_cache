#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/explain_pipeline.go \
	hat/hatSql/explain_pipeline_test.go \
	hat/hatSql/explain_pipeline_benchmark_test.go
