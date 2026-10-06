#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-mu035-go-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test -v ./hat/hatPipeline -run 'TestMU035SnapshotCutover' -count=1
GOCACHE="$cache_dir" go test ./hat/hatPipeline -count=1
GOCACHE="$cache_dir" go test -race ./hat/hatPipeline -run 'TestMU035SnapshotCutover' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatPipeline
