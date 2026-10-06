#!/usr/bin/env bash
set -euo pipefail

mode="${1:-package}"
cache="$PWD/.codex-gocache-tu18"
mkdir -p "$cache"
case "$mode" in
package)
	GOCACHE="$cache" go test -tags tu18 ./hat/hatCache -count=1
	;;
race)
	GOCACHE="$cache" go test -tags tu18 -race ./hat/hatCache -run 'TestVolatileEngine' -count=1
	;;
vet)
	GOCACHE="$cache" go vet ./hat/hatCache
	;;
all)
	GOCACHE="$cache" go test -tags tu18 ./... -count=1
	;;
*)
	printf 'usage: %s package|race|vet|all\n' "$0" >&2
	exit 2
	;;
esac
