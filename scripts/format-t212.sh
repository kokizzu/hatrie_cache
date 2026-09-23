#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/t212_replica_retention_test.go \
	hat/hatCache/t212_replica_retention_baseline_benchmark_test.go \
	hat/hatCache/t212_replica_retention_benchmark_test.go
