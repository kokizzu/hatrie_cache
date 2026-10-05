#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run '^TestCompactPeer(Listener|Handshake|Session|Daemon)' -count=1 -timeout=2m
