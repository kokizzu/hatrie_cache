#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactPeerSession(CallTemplate|PayloadCompression)' -count=1
go test -race ./hat/hatPeer -run 'TestCompactPeerSession(CallTemplate|PayloadCompression)' -count=1
go vet ./hat/hatPeer
git diff --check
