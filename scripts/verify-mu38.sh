#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
go test ./hat/hatStorage
go test -race ./hat/hatStorage
go vet ./hat/hatStorage
