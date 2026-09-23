#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT
go test ./hat/hatDataStructure -run '^TestT216DeferredCompactionTracksDebtAndCompactsOnDemand$' -count=1 -v
