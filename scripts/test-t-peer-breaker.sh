#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestConnectionPoolCircuitBreaker' -count=1
