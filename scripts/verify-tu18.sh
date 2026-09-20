#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run '^TestTU18'
go vet ./hat/hatDataStructure
test -f TU18_VOLATILE_CACHE.md
