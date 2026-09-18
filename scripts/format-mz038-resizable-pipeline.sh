#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz038_resizable_pipeline.go \
  hat/hatPipeline/mz038_resizable_pipeline_test.go
