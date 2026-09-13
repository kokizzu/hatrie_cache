#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH050(JSONRowsDecode|RowBinaryStreamDecode|RowBinaryStreamMaterializedDecode|RowBinaryImport)$' -benchmem -count=5
