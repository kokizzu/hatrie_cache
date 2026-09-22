#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^$' -bench '^Benchmark(T?R034SQLTransactionSavepoint|T232SQLTransactionScope)$' -benchmem -benchtime=3s -count=3
