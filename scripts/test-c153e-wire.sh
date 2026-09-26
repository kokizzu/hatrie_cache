#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153e-test-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT HUP INT TERM

GOCACHE="$cache_dir" go test ./hat/hatTopology -run '^TestC153ePartitionOwnershipConsensusVoteWire' -count=1 -v
