#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu17-verify-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatStorage ./hat/hatCache -run 'Test(EngineProfiles|ProfileForBackend|OpenPersistentStoreWithProfile)' -count=1
GOCACHE="$cache_dir" go test -race ./hat/hatStorage ./hat/hatCache -run 'Test(EngineProfiles|ProfileForBackend|OpenPersistentStoreWithProfile)' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatStorage ./hat/hatCache
