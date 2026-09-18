#!/usr/bin/env bash
set -euo pipefail

test -s TR045_COMPACT_PEER_CIRCUIT_BREAKER.md
rg -n 'TR-045|TR045_COMPACT_PEER_CIRCUIT_BREAKER|CompactPeerCircuitBreaker' README.md BENCHMARK.md INSPIRATION_BACKLOG.md TR045_COMPACT_PEER_CIRCUIT_BREAKER.md
rg -n 'DefaultCompactPeerCircuitBreakerFailures|DefaultCompactPeerCircuitBreakerCooldown|make benchmark-tr045-peer' TR045_COMPACT_PEER_CIRCUIT_BREAKER.md hat/hatPeer/circuit_breaker.go
