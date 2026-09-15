#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkCompactProtocolMarshalInto|BenchmarkCompactPeerSessionCall($|Template$)' -benchmem -count=5
