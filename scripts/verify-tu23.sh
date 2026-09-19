#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
go test ./hat/hatDataStructure -count=1
go test -race ./hat/hatDataStructure -count=1
go vet ./hat/hatDataStructure
bash scripts/benchmark-tu23.sh
