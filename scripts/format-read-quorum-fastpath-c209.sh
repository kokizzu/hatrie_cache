#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/read_quorum.go \
  hat/hatReplication/read_quorum_fastpath_test.go \
  hat/hatReplication/read_quorum_fastpath_benchmark_test.go
