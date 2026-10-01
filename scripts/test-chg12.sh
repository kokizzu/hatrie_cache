#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
	go test ./hat/hatReplication -run 'ConflictPolicyRegistry'
	;;
package)
	go test ./hat/hatReplication
	;;
race)
	go test -race ./hat/hatReplication
	;;
vet)
	go vet ./hat/hatReplication
	;;
all)
	go test ./hat/hatReplication -run 'ConflictPolicyRegistry'
	go test ./hat/hatReplication
	go test -race ./hat/hatReplication
	go vet ./hat/hatReplication
	;;
*)
	printf 'unknown chg12 test mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
