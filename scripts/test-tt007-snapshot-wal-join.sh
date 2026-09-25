#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt007-go-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test . ./hat/hatCache -run 'TestJoinCommandJournalSnapshot' -count=1
