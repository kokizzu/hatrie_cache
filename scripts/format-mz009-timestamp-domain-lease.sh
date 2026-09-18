#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz009_timestamp_domain_lease.go \
  hat/hatPipeline/mz009_timestamp_domain_lease_benchmark_test.go \
  hat/hatPipeline/mz009_timestamp_domain_lease_test.go
