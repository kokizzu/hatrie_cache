#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatAuth/resource_registry.go \
  hat/hatAuth/mu020_secret_resources_test.go \
  hat/hatAuth/mu020_secret_resource_baseline_benchmark_test.go
