#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run '^TestCompactPeerSession' -count=1
