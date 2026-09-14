#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -run '^$' -bench '^BenchmarkCH028DictionaryVersionedLookup$' -benchmem -count=5
