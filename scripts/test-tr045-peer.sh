#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestCompactPeerCircuitBreaker' -count=1
