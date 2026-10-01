#!/usr/bin/env bash
set -euo pipefail

mode="${1:?mode is required}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$repo_root/.t103-native-tmp"

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
cleanup
mkdir -p "$tmp_dir"
export TMPDIR="$tmp_dir"
export GOCACHE="$tmp_dir/go-cache"

case "$mode" in
test-red)
	go test ./hat/hatExtension -run 'TestNativeExtension' -count=1
	;;
benchmark-baseline)
	go test ./hat/hatExtension -run '^$' -bench '^BenchmarkNativeExtensionMapResolve$' -benchmem -count=5
	;;
benchmark-after)
	go test ./hat/hatExtension -run '^$' -bench '^BenchmarkNativeExtension(RegistryResolve|RegistryMetadata|ManifestNormalize)$' -benchmem -count=5
	;;
test)
	go test ./hat/hatExtension -count=1
	;;
race)
	go test -race ./hat/hatExtension -count=1
	;;
vet)
	go vet ./hat/hatExtension
	;;
*)
	printf 'unknown T103 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
