#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactPeerSession(CallTemplate|PayloadCompression)' -count=1
