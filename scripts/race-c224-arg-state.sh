#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  make cleanup-hatrie-tmp-after-test >/dev/null
}
trap cleanup EXIT

go test -race ./hat/hatDataStructure -run '^TestC224Arg' -count=1
