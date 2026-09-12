#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
gofmt -w hat/hatDataStructure/ordered_snapshot_cursor.go hat/hatDataStructure/ordered_snapshot_cursor_test.go
