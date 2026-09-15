#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactPeerListener|TestCompactPeerSessionOptionsForNegotiatedHandshake' -count=1
