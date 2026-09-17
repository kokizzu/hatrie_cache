#!/usr/bin/env bash
set -euo pipefail

exec gofmt -w \
	hat/hatSql/mu023_cluster_admission.go \
	hat/hatSql/mu023_cluster_admission_test.go \
	hat/hatSql/mu023_cluster_admission_baseline_benchmark_test.go \
	hat/hatSql/mu023_cluster_admission_benchmark_test.go
