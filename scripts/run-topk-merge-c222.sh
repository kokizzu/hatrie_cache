#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-topk-merge-c222.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

cp -a "$root/." "$workdir/"
rm -rf "$workdir/.git"
mkdir -p "$workdir/hat/hatSql"
git -C "$root" show HEAD:hat/hatSql/asof_join.go > "$workdir/hat/hatSql/asof_join.go"

cd "$workdir"
case "$mode" in
test)
	go test ./hat/hatCache -run 'TestTopK(Merge|AggregateState)' -count=1
	;;
package)
	go test ./hat/hatCache -count=1
	;;
race)
	go test -race ./hat/hatCache -run 'TestTopK(Merge|AggregateState)' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench 'TopKMerge' -benchmem -benchtime="${BENCHTIME:-250ms}" -count="${BENCHCOUNT:-5}"
	;;
*)
	printf 'usage: %s {test|package|race|vet|benchmark}\n' "$0" >&2
	exit 2
	;;
esac
