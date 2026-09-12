#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
go test -C "$root" ./hat/hatPeer -run '^TestCompact' -count=1
