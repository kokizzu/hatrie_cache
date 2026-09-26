#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t042_parallel_replay.go \
  hat/hatReplication/t042_parallel_replay_test.go \
  hat/hatReplication/t042_parallel_replay_benchmark_test.go
