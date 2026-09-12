#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
go test ./hat/hatTopology -count=1
go test -race ./hat/hatTopology -count=1
go vet ./hat/hatTopology
