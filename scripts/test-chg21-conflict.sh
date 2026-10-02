#!/usr/bin/env bash
set -euo pipefail

mode="${1:?expected format, red, unit, race, vet, package, or benchmark}"
tmp_dir="$(mktemp -d /tmp/hatrie-cache-chg21-test-XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

export GOCACHE="$tmp_dir/go-build"

case "$mode" in
format)
	gofmt -w hat/hatReplication/conflict_introspection.go hat/hatReplication/conflict_introspection_test.go
	;;
red)
	go test ./hat/hatReplication -run 'TestConflictIntrospectionLog' -count=1
	;;
unit)
	go test ./hat/hatReplication -run 'TestConflictIntrospectionLog' -count=1
	;;
race)
	go test -race ./hat/hatReplication -run 'TestConflictIntrospectionLog' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
package)
	go test ./hat/hatReplication -count=1
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench 'ConflictIntrospection|ResolveConflictVersion' -benchmem -benchtime=100ms -count=1
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
