#!/usr/bin/env bash
set -u

mode=${1:-test}
cache=$(mktemp -d /tmp/hatrie-t028-gocache.XXXXXX)
tmp=$(mktemp -d /tmp/hatrie-t028-gotmp.XXXXXX)
cleanup() {
	rm -rf "$cache" "$tmp"
}
trap cleanup EXIT

case "$mode" in
test)
	GOCACHE="$cache" GOTMPDIR="$tmp" go test ./hat/hatPeer -run 'TestConnectionPoolLifecycle' -count=1
	;;
package)
	GOCACHE="$cache" GOTMPDIR="$tmp" go test ./hat/hatPeer -run 'TestConnectionPool|TestPeerLifecycle' -count=1
	;;
compile)
	GOCACHE="$cache" GOTMPDIR="$tmp" go test ./hat/hatPeer -run '^$' -count=1
	;;
race)
	GOCACHE="$cache" GOTMPDIR="$tmp" go test -race ./hat/hatPeer -run 'TestConnectionPoolLifecycle' -count=1
	;;
vet)
	GOCACHE="$cache" GOTMPDIR="$tmp" go vet ./hat/hatPeer
	;;
format)
	gofmt -w hat/hatPeer/t028_connection_pool_lifecycle_test.go
	;;
*)
	printf 'usage: %s {test|package|compile|race|vet|format}\n' "$0" >&2
	exit 2
	;;
esac
