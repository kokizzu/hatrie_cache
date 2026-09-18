#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^(BenchmarkCompactPeerSessionCall|BenchmarkCompactPeerCircuitBreakerCall|BenchmarkCompactPeerSessionRemoteError|BenchmarkCompactPeerCircuitBreakerOpen)$' -benchmem -count=5
