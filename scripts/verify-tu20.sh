#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run '^TestTU20'
go vet ./hat/hatDataStructure
test -f TU20_ONLINE_SPACE_UPGRADE.md
