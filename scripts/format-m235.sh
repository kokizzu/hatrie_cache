#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m235
gofmt -w hat/hatSql/m235_dependency_graph.go hat/hatSql/m235_dependency_graph_test.go hat/hatSql/m235_dependency_graph_benchmark_test.go
