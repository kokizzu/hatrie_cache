#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestCompactPeerSession' -count=1
