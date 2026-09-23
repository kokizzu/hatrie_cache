#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT
go test -tags=t216 ./hat/hatDataStructure -run '^TestT216' -count=1
