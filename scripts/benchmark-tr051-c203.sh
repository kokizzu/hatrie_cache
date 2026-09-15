#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkPerformCompactPeerHandshake($|WithCompressionFeature$)' -benchmem -count=5
