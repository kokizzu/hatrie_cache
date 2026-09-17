#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/sql_sink_commit.go \
  hat/hatSql/sql_sink_exactly_once.go \
  hat/hatSql/sql_sink_exactly_once_codec.go \
  hat/hatSql/mu018_exactly_once_sink_test.go \
  hat/hatSql/mu018_exactly_once_sink_baseline_benchmark_test.go \
  hat/hatSql/mu018_exactly_once_sink_benchmark_test.go
