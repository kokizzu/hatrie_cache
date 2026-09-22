#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestCompactPeerSessionCallBatch' -count=1
