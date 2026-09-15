#!/usr/bin/env bash
set -euo pipefail

temporary_root="$(mktemp -d)"
cleanup() {
	rm -rf "$temporary_root"
}
trap cleanup EXIT

cp -a . "$temporary_root/source"
git show HEAD:hat/hatSql/asof_join.go > "$temporary_root/source/hat/hatSql/asof_join.go"
(cd "$temporary_root/source" && go test ./hat/hatCache -count=1)
