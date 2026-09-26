#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-go-cache-t047j.XXXXXX)
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatCache -run '^TestTU047HTTPClusterWriteCommit' -count=1
