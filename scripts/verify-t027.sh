#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t027-gocache.XXXXXX")"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t027-gotmp.XXXXXX")"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

run_go() {
	GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go "$@"
}

cd "$repo_dir"
case "${1:-test}" in
format)
	gofmt -w \
		hat/hatPeer/remote_watch.go \
		hat/hatPeer/remote_watch_test.go \
		hat/hatPeer/t027_remote_watch_baseline_benchmark_test.go \
		hat/hatCache/peer_watch.go \
		hat/hatCache/t027_remote_watch_test.go \
		hat/hatCache/t027_remote_watch_benchmark_test.go
;;
test)
	run_go test ./hat/hatPeer ./hat/hatCache -run '^TestT027' -count=1
;;
race)
	run_go test -race ./hat/hatPeer ./hat/hatCache -run '^TestT027' -count=1
;;
vet)
	run_go vet ./hat/hatPeer ./hat/hatCache
;;
full)
	run_go test ./hat/hatPeer ./hat/hatCache
;;
*)
	echo "unknown T-U27 verification mode: $1" >&2
	exit 2
;;
esac
