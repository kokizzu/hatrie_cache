#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestPersistentDeleteBitmap(AppliesVerticalTTLDeletes|VerticalTTLDeleteBoundsAndValidation)$' -count=1
