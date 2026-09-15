#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkCompactRequestTemplateMarshal|BenchmarkCompactPeerSessionCall($|Template$)' -benchmem -count=5
