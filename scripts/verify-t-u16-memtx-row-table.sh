#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -count=1
go test -race ./hat/hatDataStructure -run 'TestMemtxRowTable' -count=1
go vet ./hat/hatDataStructure
