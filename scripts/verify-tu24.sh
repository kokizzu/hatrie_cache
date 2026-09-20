#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run '^TestTU24'
go vet ./hat/hatDataStructure
test -f TU24_CONDITIONAL_INDEX_CATALOG.md
