#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestTU16MemtxTable' -count=1
