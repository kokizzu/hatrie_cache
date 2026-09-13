#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatDataStructure -count=1
go test -race ./hat/hatDataStructure -run '^TestOrderedIndex(Reverse|Iterator|Snapshot)' -count=1
go vet ./hat/hatDataStructure
