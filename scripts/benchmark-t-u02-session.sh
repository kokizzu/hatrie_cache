#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkCompactPeerSessionCall$' -benchmem -count=3
