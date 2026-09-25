#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle -run '^$' -bench '^BenchmarkCH005PartCatalog(Without|With)DeleteBitmap$' -benchmem -count=5
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH005PatchState/(Marshal|MarshalWithManifest)$' -benchmem -count=5
