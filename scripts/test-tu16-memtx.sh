#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTU16MemtxTable' -count=1
