#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mutation_dependency_graph.go \
  hat/hatPipeline/mutation_dependency_graph_test.go \
  hat/hatPipeline/mutation_dependency_graph_performance_test.go
