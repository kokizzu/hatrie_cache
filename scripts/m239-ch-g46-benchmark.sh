#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary \
  -run '^$' \
  -bench '^BenchmarkCH046DictionaryMissing(Baseline|NegativeCache)$' \
  -benchmem \
  -count=5
