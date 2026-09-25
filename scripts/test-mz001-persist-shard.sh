#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-mz001-test-XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^TestMZ001DurablePersistShard' -count=1 -v
