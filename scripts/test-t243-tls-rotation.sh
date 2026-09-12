#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestCompactPeer(TLS|MutualTLS)' -count=1
