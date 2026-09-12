#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkCompactPeerTLS' -benchmem -count=5 -benchtime=1s
