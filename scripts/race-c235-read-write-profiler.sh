#!/usr/bin/env bash
set -euo pipefail
cleanup() {
  make cleanup-hatrie-tmp-after-test >/dev/null
}
trap cleanup EXIT
go test -race ./hat/hatSql -run '^TestC235ReadWriteProfiler' -count=1
