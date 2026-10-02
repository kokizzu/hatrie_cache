#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache="${TMPDIR:-/tmp}/hatrie-cache-round62-ch059-${mode}-gocache"
rm -rf "$cache"
trap 'rm -rf "$cache"' EXIT

case "$mode" in
  test)
    GOCACHE="$cache" go test ./hat/hatCache -run 'TestCH05[89]' -count=1
    ;;
  race)
    GOCACHE="$cache" go test -race ./hat/hatCache -run 'TestCH05[89]' -count=1
    ;;
  vet)
    GOCACHE="$cache" go vet ./hat/hatCache
    ;;
  *)
    printf 'usage: %s test|race|vet\n' "$0" >&2
    exit 2
    ;;
esac
