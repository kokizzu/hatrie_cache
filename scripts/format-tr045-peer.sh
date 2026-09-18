#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/circuit_breaker.go hat/hatPeer/circuit_breaker_test.go hat/hatPeer/circuit_breaker_benchmark_test.go
