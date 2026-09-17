#!/bin/sh
set -eu

gofmt -w \
  hat/hatPipeline/dataflow_graph.go \
  hat/hatPipeline/dataflow_graph_benchmark_test.go \
  hat/hatPipeline/dataflow_graph_test.go \
  hat/hatPipeline/pipeline.go
