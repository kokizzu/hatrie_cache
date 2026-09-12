#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

go test ./hat/hatPeer -run '^$' -bench '^Benchmark(CompactPeerSessionCall|CompactPeerStreamCall)$' -benchmem -count=3
