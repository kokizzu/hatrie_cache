#!/usr/bin/env bash
set -euo pipefail

git commit --only \
  -m 'feat: add dynamic dataflow worker scaling [skip ci]' \
  -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md \
  Makefile \
  README.md \
  hat/hatPipeline/mz038_resizable_pipeline.go \
  hat/hatPipeline/mz038_resizable_pipeline_benchmark_test.go \
  hat/hatPipeline/mz038_resizable_pipeline_test.go \
  scripts/benchmark-mz038-resizable-pipeline.sh \
  scripts/commit-mz038-resizable-pipeline.sh \
  scripts/format-mz038-resizable-pipeline-benchmark.sh \
  scripts/format-mz038-resizable-pipeline.sh \
  scripts/push-mz038-resizable-pipeline.sh \
  scripts/race-mz038-resizable-pipeline.sh \
  scripts/stage-mz038-resizable-pipeline.sh \
  scripts/test-mz038-resizable-pipeline-package.sh \
  scripts/test-mz038-resizable-pipeline.sh \
  scripts/verify-mz038-resizable-pipeline-docs.sh \
  scripts/vet-mz038-resizable-pipeline.sh
