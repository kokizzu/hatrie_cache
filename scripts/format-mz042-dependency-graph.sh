#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/materialized.go hat/hatSql/materialized_dependency_graph_test.go hat/hatSql/materialized_dependency_benchmark_test.go
