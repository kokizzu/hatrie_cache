#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-window.XXXXXX")"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
GOCACHE="$tmp_dir/gocache" go test -race ./hat/hatSql -run '^TestM065SQL(WindowFrameMatchesRecompute|IncrementalWindowPreservesNullAverageAndDescendingOrder|IncrementalWindowRecoversAfterNonFiniteValueLeavesFrame)$' -count=1
