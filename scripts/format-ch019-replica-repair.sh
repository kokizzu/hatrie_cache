#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/ch019_replica_part_repair.go \
  hat/hatReplication/ch019_replica_part_repair_test.go \
  hat/hatReplication/ch019_replica_part_repair_benchmark_test.go
