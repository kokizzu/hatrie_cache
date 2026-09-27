#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz004_durable_compaction_jobs.go \
  hat/hatPipeline/mz004_durable_compaction_jobs_test.go \
  hat/hatPipeline/mz004_durable_compaction_jobs_benchmark_test.go
