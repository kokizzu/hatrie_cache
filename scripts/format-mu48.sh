#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/connector_health.go \
  hat/hatPipeline/connector_lifecycle.go \
  hat/hatPipeline/m_u48_connector_health_test.go \
  hat/hatPipeline/m_u48_connector_health_benchmark_test.go
