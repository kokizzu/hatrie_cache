#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactProtocolMarshalIntoMatchesMarshal|TestCompactPeerSession(CallTemplate|PayloadCompression)' -count=1
