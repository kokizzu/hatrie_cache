#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mu023_cluster_admission.go hat/hatSql/mu024_workload_priority_test.go hat/hatSql/mu024_workload_priority_baseline_benchmark_test.go hat/hatSql/mu024_workload_priority_benchmark_test.go
