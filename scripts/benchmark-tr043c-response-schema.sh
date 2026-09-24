#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'Compact(ResponseSchema|PeerSessionCallTemplate)' -benchmem -count=5
