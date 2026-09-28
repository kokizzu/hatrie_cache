#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatRate -run '^TestRateLimiterAllowN' -count=1
