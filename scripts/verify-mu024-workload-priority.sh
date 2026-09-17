#!/usr/bin/env bash
set -euo pipefail

test -s MU024_WORKLOAD_PRIORITIES.md
test -s hat/hatSql/mu023_cluster_admission.go
test -s hat/hatSql/mu024_workload_priority_test.go
test -s hat/hatSql/mu024_workload_priority_benchmark_test.go
rg -n 'SQLClusterWorkload|MaxSQLClusterAdmissionPriority|WorkloadClass' hat/hatSql/mu023_cluster_admission.go MU024_WORKLOAD_PRIORITIES.md README.md
rg -n 'M-U24.*Adopted' PRODUCT_IDEA_GAPS.md
rg -n 'mu-024-workload-classes-and-priorities|BenchmarkMU024' BENCHMARK.md
