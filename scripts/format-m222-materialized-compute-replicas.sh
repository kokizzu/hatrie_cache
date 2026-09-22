#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m222_materialized_compute_replica_test.go
gofmt -w hat/hatSql/m222_materialized_compute_replica.go
gofmt -w hat/hatSql/m222_materialized_compute_replica_benchmark_test.go
