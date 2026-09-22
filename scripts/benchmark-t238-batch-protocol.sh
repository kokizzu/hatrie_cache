#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkT238CompactPeer' -benchmem -count=5 -benchtime=1s
