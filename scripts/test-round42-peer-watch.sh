#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactPeerConfigWatch' -count=1
