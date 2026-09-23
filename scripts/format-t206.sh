#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/tu206_deterministic_replica_bootstrap.go hat/hatReplication/tu206_deterministic_replica_bootstrap_test.go hat/hatReplication/tu206_deterministic_replica_bootstrap_benchmark_test.go
