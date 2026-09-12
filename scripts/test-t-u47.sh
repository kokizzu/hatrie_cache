#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestConnectionPoolCloseCancelsActiveHandlerContext$' -count=1
