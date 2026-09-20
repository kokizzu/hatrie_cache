#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run '^TestTU21'
go vet ./hat/hatDataStructure
test -f TU21_VERSIONED_MIGRATION_MANAGER.md
