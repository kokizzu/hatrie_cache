#!/bin/sh
set -eu

go test ./hat/hatPeer -run '^$' -bench '^(BenchmarkCompactPeerSessionCall|BenchmarkCompactPeerSessionRequestCancellation)$' -benchmem -count=5
