#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ036_DATAFLOW_OPERATOR_PLACEMENT.md \
  Makefile \
  README.md \
  hat/hatPipeline/mz036_dataflow_placement.go \
  hat/hatPipeline/mz036_dataflow_placement_benchmark_test.go \
  hat/hatPipeline/mz036_dataflow_placement_test.go \
  scripts/benchmark-mz036-dataflow-placement.sh \
  scripts/commit-mz036-dataflow-placement.sh \
  scripts/format-mz036-dataflow-placement-benchmark.sh \
  scripts/format-mz036-dataflow-placement.sh \
  scripts/push-mz036-dataflow-placement.sh \
  scripts/race-mz036-dataflow-placement.sh \
  scripts/stage-mz036-dataflow-placement.sh \
  scripts/test-mz036-dataflow-placement-package.sh \
  scripts/test-mz036-dataflow-placement.sh \
  scripts/verify-mz036-dataflow-placement-docs.sh \
  scripts/vet-mz036-dataflow-placement.sh
