#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCatalogSchemaEvolution' -benchmem -count=5
