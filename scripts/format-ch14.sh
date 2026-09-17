#!/bin/sh
set -eu

gofmt -w hat/hatSql/mutation_dependency_graph.go hat/hatSql/mutation_dependency_graph_test.go hat/hatSql/mutation_dependency_graph_benchmark_test.go
