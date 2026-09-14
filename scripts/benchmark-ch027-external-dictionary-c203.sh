#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -run '^$' -bench '^BenchmarkCH027DictionaryLookup$' -benchmem -count=5
