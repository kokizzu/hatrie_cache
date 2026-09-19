#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
baseline_ref=${M050_BASELINE_REF:-HEAD^}
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m050-baseline.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-baseline.XXXXXX.tar")
cleanup() {
	rm -rf "$stage" "$archive"
}
trap cleanup EXIT

base=$(git -C "$repo" rev-parse "$baseline_ref")
git -C "$repo" archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"
cat >"$stage/hat/hatBackup/m050_baseline_control_test.go" <<'EOF'
package hatBackup

import "testing"

var m050BaselineControlSink uint64

func BenchmarkM050BaselineControl(b *testing.B) {
	var value uint64
	for index := 0; index < b.N; index++ {
		value += uint64(index) ^ 0x9e3779b97f4a7c15
	}
	m050BaselineControlSink = value
}
EOF
cd "$stage"
go test -run '^$' -bench '^BenchmarkM050BaselineControl' -benchmem -count=5 ./hat/hatBackup
