#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestCompactPeerCircuitBreaker|TestCompactPeerSession' -count=1
