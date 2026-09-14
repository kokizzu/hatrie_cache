#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactProtocol.*Compression|TestCompactPeerSessionPayloadCompression' -count=1
