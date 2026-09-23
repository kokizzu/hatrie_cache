#!/usr/bin/env bash
set -euo pipefail

trap 'make cleanup-hatrie-tmp-after-test >/dev/null' EXIT
go test -race ./hat/hatDataStructure -run 'TestHashIndex' -count=1
