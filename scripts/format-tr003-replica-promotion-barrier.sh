#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/tr003_replica_promotion_barrier.go \
  hat/hatReplication/tr003_replica_promotion_barrier_test.go
