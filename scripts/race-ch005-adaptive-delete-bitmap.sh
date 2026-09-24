#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^(TestCH005|TestPersistentDeleteBitmap)' -count=1
