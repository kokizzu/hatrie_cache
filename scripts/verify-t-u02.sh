#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
go test ./hat/hatPeer -count=1
go test -race ./hat/hatPeer -count=1
go vet ./hat/hatPeer
