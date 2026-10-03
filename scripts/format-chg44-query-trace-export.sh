#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_trace_export.go hat/hatSql/query_trace_export_test.go hat/hatSql/query_trace_spans_benchmark_test.go
