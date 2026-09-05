#!/bin/sh
set -eu

gofmt -w hat/hatSql/query_trace.go hat/hatSql/query_trace_spans.go hat/hatSql/query_trace_spans_test.go hat/hatSql/query_trace_spans_benchmark_test.go
