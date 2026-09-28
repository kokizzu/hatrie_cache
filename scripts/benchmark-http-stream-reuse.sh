#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHttp -run '^$' -bench '^BenchmarkBinaryStreamReaderNext(Allocating|Into)$' -benchmem -count=5
