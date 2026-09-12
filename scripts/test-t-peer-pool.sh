#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^TestConnectionPool' -count=1
