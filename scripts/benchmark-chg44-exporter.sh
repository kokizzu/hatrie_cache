#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTrace -run '^$' -bench '^BenchmarkOTLPHTTPExporterExport$' -benchmem -benchtime=100ms -count=5
