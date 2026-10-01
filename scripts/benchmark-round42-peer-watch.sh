#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -v -run '^TestCompactPeerConfigWatchWireSize$' -bench 'Benchmark(ConfigWatchDirectReadOneEvent|CompactPeerConfigWatchReadOneEvent|CompactPeerConfigWatchEncode|JSONConfigWatchEncode)$' -benchmem -count=5
