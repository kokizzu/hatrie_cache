#!/bin/sh
set -eu

gofmt -w \
  hat/hatReplication/write_quorum_fastpath.go \
  hat/hatReplication/write_quorum_fastpath_test.go \
  hat/hatReplication/write_quorum_fastpath_benchmark_test.go
