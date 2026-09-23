#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT
go test -race ./hat/hatDataStructure -run '^TestT216' -count=1
