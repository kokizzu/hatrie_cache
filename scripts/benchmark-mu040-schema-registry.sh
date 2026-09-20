#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU040(LegacyIngestionBaseline|RegistryIngestion)$' -benchmem -count=5
