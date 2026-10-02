#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache=/tmp/hatrie-cache-round63-ch060-${mode}-gocache
trap 'rm -rf -- "$cache"' EXIT

case "$mode" in
test)
	GOCACHE="$cache" go test ./hat/hatCache -run 'TestCH05(8|9)|TestCH060' -count=1
	;;
race)
	GOCACHE="$cache" go test -race ./hat/hatCache -run 'TestCH05(8|9)|TestCH060' -count=1
	;;
vet)
	GOCACHE="$cache" go vet ./hat/hatCache
	;;
*)
	printf 'usage: %s {test|race|vet}\n' "$0" >&2
	exit 2
	;;
esac
