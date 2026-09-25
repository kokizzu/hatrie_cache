#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle -run '^$' -bench '^BenchmarkCH005PartCatalogWithoutDeleteBitmap$' -benchmem -count=5
