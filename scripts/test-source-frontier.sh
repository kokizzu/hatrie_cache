#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d "$PWD/.test-tmp.XXXXXX")
cleanup() {
	rm -rf -- "$tmpdir"
}
trap cleanup EXIT

TMPDIR="$tmpdir" go test ./hat/hatMetrics -run 'TestSourceFrontierRegistry' -count=1
