#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/connector_checkpoint.go \
  hat/hatPipeline/connector_lifecycle.go \
  hat/hatPipeline/m_u42_connector_checkpoint_test.go \
  hat/hatPipeline/m_u42_connector_checkpoint_benchmark_test.go \
  scripts/commit-mu42.go
