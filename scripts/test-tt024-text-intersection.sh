#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-gocache-tt024.XXXXXX)
tmp_dir=$(mktemp -d /tmp/hatrie-cache-gotmp-tt024.XXXXXX)
trap 'rm -rf -- "$cache_dir" "$tmp_dir"' EXIT
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatCache -run '^TestSQLTextPhraseIndexIntersectionAcrossFields$' -count=1
