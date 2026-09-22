#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_expiry.go \
  hat/hatPipeline/m247_frontier_expiry_error_test.go
