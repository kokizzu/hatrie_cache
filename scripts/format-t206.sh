#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t206_replica_join_admission.go \
  hat/hatReplication/t206_replica_join_admission_test.go \
  hat/hatReplication/t206_replica_join_admission_benchmark_test.go
