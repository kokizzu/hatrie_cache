#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153c-race-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatPipeline -run '^TestC153cQueuePartitionOwnershipSnapshot' -count=1
