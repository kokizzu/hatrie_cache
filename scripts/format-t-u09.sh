#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/join_bootstrap.go \
  hat/hatReplication/t_u09_join_bootstrap_test.go \
  hat/hatReplication/t_u09_join_bootstrap_benchmark_test.go
