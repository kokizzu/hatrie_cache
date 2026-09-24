#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactProtocolMarshalIntoMatchesMarshal|TestCompactResponseSchema' -count=1
