#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT233' -count=1
go test -race ./hat/hatDataStructure -run '^TestT233' -count=1
go vet ./hat/hatDataStructure
