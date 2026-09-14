#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkCompactProtocolPayload' -benchmem -benchtime=100ms -count=3
