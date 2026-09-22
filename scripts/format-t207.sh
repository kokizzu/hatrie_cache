#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t207_replica_eviction_recovery.go \
  hat/hatReplication/t207_replica_eviction_recovery_test.go \
  hat/hatReplication/t207_replica_eviction_recovery_baseline_benchmark_test.go \
  hat/hatReplication/t207_replica_eviction_recovery_benchmark_test.go
