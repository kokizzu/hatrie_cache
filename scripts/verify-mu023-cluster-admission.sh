#!/usr/bin/env bash
set -euo pipefail

test -s MU023_CLUSTER_QUERY_ADMISSION.md
test -s hat/hatSql/mu023_cluster_admission.go
test -s hat/hatSql/mu023_cluster_admission_test.go
rg -n 'SQLClusterAdmission|mu-023-cluster-query-admission' README.md MU023_CLUSTER_QUERY_ADMISSION.md BENCHMARK.md PRODUCT_IDEA_GAPS.md
rg -n 'M-U23.*Adopted' PRODUCT_IDEA_GAPS.md
