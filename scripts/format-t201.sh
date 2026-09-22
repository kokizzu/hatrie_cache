#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t201_per_space_write_quorum.go \
  hat/hatReplication/t201_per_space_write_quorum_test.go \
  hat/hatReplication/t201_per_space_write_quorum_benchmark_test.go
