#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatReplication/conflict_resolution.go hat/hatReplication/conflict_resolution_test.go
