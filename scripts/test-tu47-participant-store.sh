#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu47-test-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^TestClusterWriteCommitParticipantFileStore' -count=1
