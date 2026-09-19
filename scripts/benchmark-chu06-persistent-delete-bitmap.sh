#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkPersistentDeleteBitmap(BaselineBool(Encode|Decode)|Packed(Encode|Decode))$' -benchmem -count=5
