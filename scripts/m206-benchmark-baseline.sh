#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM206UpsertEnvelopeBaseline$' -benchmem -count=5
