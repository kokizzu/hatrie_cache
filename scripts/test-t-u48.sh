#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestRetryPolicy' -count=1
