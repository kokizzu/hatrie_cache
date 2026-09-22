#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t207_replica_eviction_recovery.go \
  hat/hatReplication/write_quorum.go \
  hat/hatReplication/t208_anonymous_replica.go \
  hat/hatReplication/t208_anonymous_replica_test.go \
  hat/hatReplication/t208_anonymous_replica_baseline_benchmark_test.go
