#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/m232_sink_frontier_checkpoint.go \
  hat/hatSql/m232_sink_frontier_checkpoint_test.go \
  hat/hatSql/m232_sink_frontier_checkpoint_benchmark_test.go
