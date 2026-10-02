#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
GOCACHE="${GOCACHE:-$root/build/mu039-go-cache}" go test ./hat/hatSql -run 'TestMU039PartitionDeclaration' -count=1
