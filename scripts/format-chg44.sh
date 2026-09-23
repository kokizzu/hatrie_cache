#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTrace/otlp_exporter.go \
  hat/hatTrace/otlp_exporter_test.go \
  hat/hatTrace/otlp_exporter_benchmark_test.go \
  hat/hatSql/query_trace_spans.go \
  hat/hatSql/query_trace_exporter_test.go
