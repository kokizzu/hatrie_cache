#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU25CatalogRebuild$' -benchmem -count=5
