#!/bin/sh
set -eu

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ042_DATAFLOW_GRAPH.md \
  README.md \
  Makefile \
  hat/hatPipeline/dataflow_graph.go \
  hat/hatPipeline/dataflow_graph_benchmark_test.go \
  hat/hatPipeline/dataflow_graph_test.go \
  hat/hatPipeline/pipeline.go \
  scripts/benchmark-mz42-dataflow-graph.sh \
  scripts/commit-mz42-dataflow-graph.sh \
  scripts/format-mz42-dataflow-graph.sh \
  scripts/push-mz42-dataflow-graph.sh \
  scripts/race-mz42-dataflow-graph.sh \
  scripts/review-mz42-dataflow-graph.sh \
  scripts/stage-mz42-dataflow-graph.sh \
  scripts/test-mz42-dataflow-graph.sh \
  scripts/test-mz42-package.sh \
  scripts/vet-mz42-dataflow-graph.sh
