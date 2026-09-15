#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestRoaringBitmapLookupBoundaries$' -count=1
