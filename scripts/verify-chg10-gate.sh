#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
cache_dir="$(mktemp -d /tmp/hatrie-cache-chg10-gocache.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

run_package() {
	GOCACHE="$cache_dir" go test ./hat/hatReplication -count=1
}

run_race() {
	GOCACHE="$cache_dir" go test -race ./hat/hatReplication -count=1
}

run_vet() {
	GOCACHE="$cache_dir" go vet ./hat/hatReplication
}

case "$mode" in
package)
	run_package
	;;
race)
	run_race
	;;
vet)
	run_vet
	;;
all)
	run_package
	run_race
	run_vet
	;;
*)
	echo "usage: $0 {package|race|vet|all}" >&2
	exit 2
	;;
esac
