#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/mutation_dependency_graph.go \
  hat/hatSql/ch014b_mutation_dependency_ready.go \
  hat/hatSql/ch014b_mutation_dependency_graph_test.go \
  hat/hatSql/ch014b_mutation_dependency_graph_benchmark_test.go \
  hat/hatSql/ch014b_mutation_dependency_ready_internal_test.go \
  hat/hatSql/ch014b_mutation_dependency_graph_baseline_benchmark_test.go
