#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
GOCACHE="${GOCACHE:-$root/build/mu039-go-cache}" go test -race ./hat/hatSql -run 'TestMU039PartitionDeclaration' -count=1
GOCACHE="${GOCACHE:-$root/build/mu039-go-cache}" go vet ./hat/hatSql
