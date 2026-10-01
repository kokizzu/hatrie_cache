#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestCompactPeerConfigWatch' -count=1
