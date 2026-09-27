#!/usr/bin/env bash
set -euo pipefail

tmp_root=$(mktemp -d /tmp/hatrie-ch011-source-notifications-race.XXXXXX)
trap 'rm -rf "$tmp_root"' EXIT
mkdir -p "$tmp_root/gocache" "$tmp_root/gotmp"
GOCACHE="$tmp_root/gocache" GOTMPDIR="$tmp_root/gotmp" go test -race ./hat/hatSql -run '^TestCH011' -count=1
