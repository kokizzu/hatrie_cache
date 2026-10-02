#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTU21BaselineSpaceCatalogLookup$' -benchmem -count=5 -timeout=120s
