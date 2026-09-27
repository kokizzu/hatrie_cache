#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch004-final-store-race.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT
mkdir -p "$temporary_root/go-tmp"
export GOCACHE="$temporary_root/go-cache"
export GOTMPDIR="$temporary_root/go-tmp"
cd "$root_dir"
go test -race ./hat/hatSql -run '^TestCH004FinalSchemaStore' -count=1
