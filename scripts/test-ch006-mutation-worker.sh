#!/usr/bin/env bash
set -euo pipefail

tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch006-worker-test.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT
mkdir -p "$tmp_root/gocache" "$tmp_root/gotmp"
GOCACHE="$tmp_root/gocache" GOTMPDIR="$tmp_root/gotmp" \
  go test ./hat/hatSql -run 'TestSQLMutationDependencyQueueRunReady' -count=1
