#!/usr/bin/env bash
set -euo pipefail

tmp_root=$(mktemp -d /tmp/hatrie-ch011-source-notifications-benchmark.XXXXXX)
trap 'rm -rf "$tmp_root"' EXIT
mkdir -p "$tmp_root/gocache" "$tmp_root/gotmp"
GOCACHE="$tmp_root/gocache" GOTMPDIR="$tmp_root/gotmp" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH011(DirectMaterializedViewRefresh|RefreshQueueNotifyAndProcess|RefreshQueueCoalescedBatch)$' -benchmem -benchtime=50x -count=5
