#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

gofmt -w \
  hat/hatPeer/compact_session.go \
  hat/hatPeer/peer_lifecycle.go \
  hat/hatPeer/peer_lifecycle_test.go
