#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPeer/connection_pool.go \
	hat/hatPeer/t237_health_baseline_benchmark_test.go \
	hat/hatPeer/t237_connection_pool_health_test.go
