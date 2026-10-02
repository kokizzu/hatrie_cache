#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${PWD}/.go-build-cache"
export GOTMPDIR="${PWD}/.go-tmp"
mkdir -p "${GOCACHE}" "${GOTMPDIR}"
go test ./hat/hatTopology -run '^$' -bench 'BenchmarkConfigWatch(LogReadWithPrefix|PeerReadRoundTrip)$' -benchmem -count=5
