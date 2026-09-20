#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run '^TestTU28' -count=1
