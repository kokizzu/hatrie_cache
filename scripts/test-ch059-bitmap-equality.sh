#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-round62-ch059-test-gocache"
rm -rf "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatCache -run '^TestCH059BitmapEquality' -count=1
